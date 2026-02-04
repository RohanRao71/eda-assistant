from sqlalchemy import create_engine
import os

def get_database_url():
    """Get database URL from environment or use default"""
    return os.getenv(
        "DATABASE_URL",
        "postgresql://postgres:5617@localhost:5432/dbms_project"
    )

def get_engine():
    """Create and return a SQLAlchemy engine"""
    return create_engine(get_database_url())