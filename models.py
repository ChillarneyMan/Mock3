from sqlalchemy import (
    Column, Integer, String, Float, Boolean, DateTime, ForeignKey, Text
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class MetricReading(Base):
    __tablename__ = "metric_readings"

    id = Column(Integer, primary_key=True, index=True)
    host_id = Column(String, nullable=False, index=True)
    metric_name = Column(String, nullable=False, index=True)
    value = Column(Float, nullable=False)
    timestamp = Column(DateTime(timezone=True), nullable=False, index=True)


class AlertRule(Base):
    __tablename__ = "alert_rules"

    id = Column(Integer, primary_key=True, index=True)
    host_id = Column(String, nullable=True)  # None = applies to all hosts
    metric_name = Column(String, nullable=False)
    operator = Column(String, nullable=False)  # ">" | "<" | ">=" | "<=" | "=="
    threshold = Column(Float, nullable=False)
    duration_seconds = Column(Integer, nullable=False)
    active = Column(Boolean, nullable=False, default=True)


class RuleState(Base):
    __tablename__ = "rule_states"

    id = Column(Integer, primary_key=True, index=True)
    rule_id = Column(Integer, ForeignKey("alert_rules.id"), nullable=False)
    host_id = Column(String, nullable=False)
    violation_started_at = Column(DateTime(timezone=True), nullable=True)
    currently_violating = Column(Boolean, nullable=False, default=False)


class AlertViolation(Base):
    __tablename__ = "alert_violations"

    id = Column(Integer, primary_key=True, index=True)
    rule_id = Column(Integer, ForeignKey("alert_rules.id"), nullable=False)
    host_id = Column(String, nullable=False)
    metric_name = Column(String, nullable=False)
    value = Column(Float, nullable=False)
    triggered_at = Column(DateTime(timezone=True), nullable=False)
    resolved_at = Column(DateTime(timezone=True), nullable=True)
