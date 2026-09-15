import pytest
from dotenv import load_dotenv

load_dotenv()

from db import engine, SessionLocal
from models import Base, AlertRule


@pytest.fixture(scope="session", autouse=True)
def deactivate_preexisting_rules():
    """Ensure tables exist and deactivate any pre-existing rules.

    create_all is idempotent — safe to call on an existing schema.
    In CI the database is fresh so tables don't exist yet; locally the
    tables already exist from the dev setup. Either way this runs before
    any AsyncClient starts the app lifespan.

    Without the deactivation step, accumulated global rules from prior
    test/manual runs match every unique test host and keep the evaluator
    busy long enough to push background tasks past the assertion window.
    """
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        db.query(AlertRule).update({"active": False})
        db.commit()
    finally:
        db.close()
