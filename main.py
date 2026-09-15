import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from typing import Any
from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy import func, or_
from dotenv import load_dotenv
from pydantic import BaseModel
from sqlalchemy.orm import Session

load_dotenv()

from db import engine, get_db, SessionLocal
from models import Base, MetricReading, AlertRule, RuleState, AlertViolation

logger = logging.getLogger("alerting")
logging.basicConfig(level=logging.INFO)


@asynccontextmanager
async def lifespan(_: FastAPI):
    Base.metadata.create_all(bind=engine)
    yield


app = FastAPI(lifespan=lifespan)

VALID_METRICS = {"cpu", "memory", "disk"}
VALID_OPERATORS = {">", "<", ">=", "<=", "=="}


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class ReadingModel(BaseModel):
    host_id: Any = None
    metric_name: Any = None
    value: Any = None
    timestamp: Any = None

    model_config = {"extra": "allow"}


class IngestRequest(BaseModel):
    readings: list[ReadingModel]


class AlertRuleIn(BaseModel):
    host_id: str | None = None
    metric_name: str
    operator: str
    threshold: float
    duration_seconds: int


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _validate_reading(raw: Any) -> tuple[MetricReading | None, str | None]:
    if not isinstance(raw, dict):
        return None, "entry must be an object"

    host_id = raw.get("host_id")
    if not isinstance(host_id, str) or not host_id.strip():
        return None, "host_id must be a non-empty string"

    metric_name = raw.get("metric_name")
    if metric_name not in VALID_METRICS:
        return None, f"metric_name must be one of {sorted(VALID_METRICS)}"

    value = raw.get("value")
    if value is None or isinstance(value, bool) or not isinstance(value, (int, float)):
        return None, "value must be numeric"

    ts_raw = raw.get("timestamp")
    if not isinstance(ts_raw, str):
        return None, "timestamp must be an ISO8601 string"
    try:
        ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
    except ValueError:
        return None, "timestamp is not a valid ISO8601 datetime"

    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)

    if ts > datetime.now(timezone.utc) + timedelta(minutes=5):
        return None, "timestamp is more than 5 minutes in the future"

    return MetricReading(
        host_id=host_id.strip(),
        metric_name=metric_name,
        value=float(value),
        timestamp=ts,
    ), None


WINDOW_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def _parse_window(window: str) -> timedelta:
    unit = window[-1]
    if unit not in WINDOW_UNITS or not window[:-1].isdigit():
        raise HTTPException(status_code=400, detail="window must be like 1h, 30m, 2d")
    return timedelta(seconds=int(window[:-1]) * WINDOW_UNITS[unit])


def _check_threshold(value: float, operator: str, threshold: float) -> bool:
    return {
        ">": value > threshold,
        "<": value < threshold,
        ">=": value >= threshold,
        "<=": value <= threshold,
        "==": value == threshold,
    }.get(operator, False)


# ---------------------------------------------------------------------------
# Rule evaluator (runs in thread pool, fired via create_task)
# ---------------------------------------------------------------------------

def _evaluate_rules_sync(host_id: str, metric_name: str) -> None:
    db: Session = SessionLocal()
    try:
        now = datetime.now(timezone.utc)

        rules = db.query(AlertRule).filter(
            AlertRule.active == True,
            AlertRule.metric_name == metric_name,
            or_(AlertRule.host_id == host_id, AlertRule.host_id == None),
        ).all()

        for rule in rules:
            # Most recent reading for this host+metric
            recent = (
                db.query(MetricReading)
                .filter(
                    MetricReading.host_id == host_id,
                    MetricReading.metric_name == metric_name,
                )
                .order_by(MetricReading.timestamp.desc())
                .first()
            )
            if recent is None:
                continue

            breaching = _check_threshold(recent.value, rule.operator, rule.threshold)

            # Get or create RuleState
            state = db.query(RuleState).filter(
                RuleState.rule_id == rule.id,
                RuleState.host_id == host_id,
            ).first()
            if state is None:
                state = RuleState(rule_id=rule.id, host_id=host_id, currently_violating=False)
                db.add(state)
                db.flush()

            if breaching:
                if not state.currently_violating:
                    # Start the violation timer
                    state.currently_violating = True
                    state.violation_started_at = now
                else:
                    elapsed = (now - state.violation_started_at).total_seconds()
                    if elapsed >= rule.duration_seconds:
                        # Sustained — log once (no open violation already)
                        open_v = db.query(AlertViolation).filter(
                            AlertViolation.rule_id == rule.id,
                            AlertViolation.host_id == host_id,
                            AlertViolation.resolved_at == None,
                        ).first()
                        if open_v is None:
                            violation = AlertViolation(
                                rule_id=rule.id,
                                host_id=host_id,
                                metric_name=metric_name,
                                value=recent.value,
                                triggered_at=now,
                            )
                            db.add(violation)
                            logger.warning(
                                "ALERT rule_id=%s host=%s %s%s%s (sustained %ss)",
                                rule.id, host_id, metric_name,
                                rule.operator, rule.threshold, int(elapsed),
                            )
            else:
                if state.currently_violating:
                    state.currently_violating = False
                    state.violation_started_at = None
                    # Resolve any open violation
                    db.query(AlertViolation).filter(
                        AlertViolation.rule_id == rule.id,
                        AlertViolation.host_id == host_id,
                        AlertViolation.resolved_at == None,
                    ).update({"resolved_at": now})

        db.commit()
    except Exception:
        logger.exception("evaluate_rules failed for host=%s metric=%s", host_id, metric_name)
        db.rollback()
    finally:
        db.close()


async def evaluate_rules(host_id: str, metric_name: str) -> None:
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _evaluate_rules_sync, host_id, metric_name)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.get("/")
def root():
    return {"status": "ok"}


@app.post("/metrics/ingest")
async def ingest(body: IngestRequest, db: Session = Depends(get_db)):
    accepted = 0
    rejected = []
    affected: set[tuple[str, str]] = set()

    for i, raw in enumerate(body.readings):
        reading, error = _validate_reading(raw.model_dump())
        if error:
            rejected.append({"index": i, "reason": error})
        else:
            db.add(reading)
            affected.add((reading.host_id, reading.metric_name))
            accepted += 1

    db.commit()

    for host_id, metric_name in affected:
        asyncio.create_task(evaluate_rules(host_id, metric_name))

    return {"accepted": accepted, "rejected": rejected}


@app.get("/metrics/query")
def query_metrics(
    host_id: str = Query(...),
    metric: str = Query(...),
    window: str = Query(...),
    agg: str | None = Query(default=None),
    db: Session = Depends(get_db),
):
    if metric not in VALID_METRICS:
        raise HTTPException(status_code=400, detail=f"metric must be one of {sorted(VALID_METRICS)}")
    if agg is not None and agg not in ("min", "max", "avg"):
        raise HTTPException(status_code=400, detail="agg must be one of min, max, avg")

    since = datetime.now(timezone.utc) - _parse_window(window)

    if agg is None:
        rows = (
            db.query(MetricReading)
            .filter(
                MetricReading.host_id == host_id,
                MetricReading.metric_name == metric,
                MetricReading.timestamp >= since,
            )
            .order_by(MetricReading.timestamp)
            .all()
        )
        return {
            "host_id": host_id,
            "metric": metric,
            "readings": [{"timestamp": r.timestamp.isoformat(), "value": r.value} for r in rows],
        }

    agg_fn = {"min": func.min, "max": func.max, "avg": func.avg}[agg]
    result = db.query(agg_fn(MetricReading.value)).filter(
        MetricReading.host_id == host_id,
        MetricReading.metric_name == metric,
        MetricReading.timestamp >= since,
    ).scalar()

    return {"host_id": host_id, "metric": metric, "agg": agg, "value": result}


@app.post("/alerts/rules", status_code=201)
def create_rule(body: AlertRuleIn, db: Session = Depends(get_db)):
    if body.metric_name not in VALID_METRICS:
        raise HTTPException(400, detail=f"metric_name must be one of {sorted(VALID_METRICS)}")
    if body.operator not in VALID_OPERATORS:
        raise HTTPException(400, detail=f"operator must be one of {sorted(VALID_OPERATORS)}")
    if body.duration_seconds <= 0:
        raise HTTPException(400, detail="duration_seconds must be positive")

    rule = AlertRule(**body.model_dump(), active=True)
    db.add(rule)
    db.commit()
    db.refresh(rule)
    return {
        "id": rule.id,
        "host_id": rule.host_id,
        "metric_name": rule.metric_name,
        "operator": rule.operator,
        "threshold": rule.threshold,
        "duration_seconds": rule.duration_seconds,
        "active": rule.active,
    }


@app.get("/alerts/rules")
def list_rules(db: Session = Depends(get_db)):
    rules = db.query(AlertRule).all()
    return [
        {
            "id": r.id,
            "host_id": r.host_id,
            "metric_name": r.metric_name,
            "operator": r.operator,
            "threshold": r.threshold,
            "duration_seconds": r.duration_seconds,
            "active": r.active,
        }
        for r in rules
    ]


@app.get("/alerts/violations")
def list_violations(db: Session = Depends(get_db)):
    violations = db.query(AlertViolation).order_by(AlertViolation.triggered_at.desc()).all()
    return [
        {
            "id": v.id,
            "rule_id": v.rule_id,
            "host_id": v.host_id,
            "metric_name": v.metric_name,
            "value": v.value,
            "triggered_at": v.triggered_at.isoformat(),
            "resolved_at": v.resolved_at.isoformat() if v.resolved_at else None,
        }
        for v in violations
    ]
