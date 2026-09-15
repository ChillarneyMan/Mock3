from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os

load_dotenv()

database_url_raw = os.environ.get("DATABASE_URL", "<<MISSING>>")
print(f"[DEBUG-CHECK-1] DATABASE_URL raw value: {database_url_raw!r}")

database_url = database_url_raw.strip().strip('"').strip("'")

if database_url.startswith("postgres://"):
    database_url = database_url.replace("postgres://", "postgresql://", 1)

print(f"[DEBUG-CHECK-1] DATABASE_URL after cleanup: {database_url!r}")

engine = create_engine(database_url)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()