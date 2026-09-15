import pytest
from dotenv import load_dotenv

load_dotenv()

from db import SessionLocal
from models import AlertRule


@pytest.fixture(scope="session", autouse=True)
def deactivate_preexisting_rules():
    """Deactivate all rules that exist before this session starts.

    Without this, accumulated global rules from prior test/manual runs
    match every unique test host and keep the evaluator busy long enough
    to push background tasks past the assertion window.
    """
    db = SessionLocal()
    try:
        db.query(AlertRule).update({"active": False})
        db.commit()
    finally:
        db.close()
