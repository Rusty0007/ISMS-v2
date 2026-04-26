from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, DeclarativeBase
from app.config import settings

engine = create_engine(
    settings.database_url,
    # Keep up to 10 connections open (idle)
    pool_size=10,
    # Allow up to 20 extra connections under burst load
    max_overflow=20,
    # Raise after 30 s if no connection is available — prevents silent hangs
    pool_timeout=30,
    # Ping the connection before handing it out; drops stale connections
    # that went away while sitting in the pool
    pool_pre_ping=True,
    # Recycle connections older than 30 min to avoid hitting PostgreSQL's
    # idle-in-transaction session timeout
    pool_recycle=1800,
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

class Base(DeclarativeBase):
    pass

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()