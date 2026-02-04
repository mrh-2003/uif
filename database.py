import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager

DATABASE_URL = os.getenv(
    'DATABASE_URL',
    'postgresql://postgres:admin@localhost:5432/uif'
)

engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_size=10, max_overflow=20)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

@contextmanager
def get_db():
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

def execute_query(query, params=None):
    with get_db() as db:
        result = db.execute(query, params or {})
        if result.returns_rows:
            return result.fetchall()
        return result.rowcount

def execute_many(query, data):
    with get_db() as db:
        db.execute(query, data)
        db.commit()
