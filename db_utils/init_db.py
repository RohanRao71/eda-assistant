from sqlalchemy import text
import os


def init_postgresql(engine):
    """
    Initialize all required PostgreSQL tables if they don't exist.
    Matches the exact schema from your database.
    Safe to run multiple times (uses IF NOT EXISTS).
    """

    schema_sql = """
    -- User details table
    CREATE TABLE IF NOT EXISTS user_details (
        user_id SERIAL PRIMARY KEY,
        username VARCHAR(150) NOT NULL,
        created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
        last_active TIMESTAMP WITHOUT TIME ZONE,
        password_hash TEXT,
        CONSTRAINT user_details_username_unique UNIQUE (username),
        CONSTRAINT username_not_empty CHECK (LENGTH(TRIM(BOTH FROM username)) > 0),
        CONSTRAINT password_not_empty CHECK (LENGTH(TRIM(BOTH FROM password_hash)) > 0)
    );

    -- Datasets metadata table
    CREATE TABLE IF NOT EXISTS datasets_metadata (
        dataset_id SERIAL PRIMARY KEY,
        dataset_name VARCHAR(255) NOT NULL,
        file_path VARCHAR(500),
        upload_date TIMESTAMP WITHOUT TIME ZONE NOT NULL,
        num_rows INTEGER NOT NULL,
        num_columns INTEGER NOT NULL,
        owner_user_id INTEGER NOT NULL,
        table_name VARCHAR(255),
        column_names TEXT[],
        CONSTRAINT fk_owner_user FOREIGN KEY (owner_user_id) 
            REFERENCES user_details(user_id) ON DELETE SET NULL
    );

    -- Projects table
    CREATE TABLE IF NOT EXISTS projects (
        project_id SERIAL PRIMARY KEY,
        project_name VARCHAR(255),
        description TEXT,
        owner_user_id INTEGER,
        dataset_id INTEGER,
        created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
        CONSTRAINT projects_dataset_id_key UNIQUE (dataset_id),
        CONSTRAINT projects_owner_user_id_fkey FOREIGN KEY (owner_user_id) 
            REFERENCES user_details(user_id),
        CONSTRAINT projects_dataset_id_fkey FOREIGN KEY (dataset_id) 
            REFERENCES datasets_metadata(dataset_id) ON DELETE SET NULL
    );

    -- Dataset column details table
    CREATE TABLE IF NOT EXISTS dataset_column_details (
        detail_id SERIAL PRIMARY KEY,
        dataset_id INTEGER NOT NULL,
        column_name VARCHAR(255) NOT NULL,
        pandas_dtype VARCHAR(100),
        column_type VARCHAR(50),
        mean DOUBLE PRECISION,
        median DOUBLE PRECISION,
        std_dev DOUBLE PRECISION,
        min_value DOUBLE PRECISION,
        max_value DOUBLE PRECISION,
        missing_values INTEGER,
        unique_value_count INTEGER,
        distinct_categories JSONB,
        computed_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
        min_datetime TIMESTAMP WITHOUT TIME ZONE,
        max_datetime TIMESTAMP WITHOUT TIME ZONE,
        CONSTRAINT dataset_column_details_dataset_id_fkey FOREIGN KEY (dataset_id) 
            REFERENCES datasets_metadata(dataset_id) ON DELETE CASCADE
    );

    -- Indexes for better performance
    CREATE INDEX IF NOT EXISTS idx_projects_owner ON projects(owner_user_id);
    CREATE INDEX IF NOT EXISTS idx_datasets_owner ON datasets_metadata(owner_user_id);
    CREATE INDEX IF NOT EXISTS idx_column_details_dataset ON dataset_column_details(dataset_id);
    """

    try:
        with engine.begin() as conn:
            for statement in schema_sql.split(';'):
                if statement.strip():
                    conn.execute(text(statement))

        print("✅ PostgreSQL schema initialized successfully")
        return True

    except Exception as e:
        print(f"❌ Error initializing PostgreSQL: {e}")
        print(f"   Details: {str(e)}")
        return False


def init_all_databases():
    """Initialize both PostgreSQL and MongoDB"""

    # PostgreSQL
    db_url = os.getenv(
        "DATABASE_URL",
        "postgresql://postgres:5617@localhost:5432/dbms_project"
    )

    print("Initializing PostgreSQL...")
    try:
        from db_utils.db_config import get_engine
        pg_engine = get_engine()
        pg_success = init_postgresql(pg_engine)
    except Exception as e:
        print(f"❌ PostgreSQL connection failed: {e}")
        pg_success = False

    # MongoDB
    print("\nInitializing MongoDB...")
    try:
        from mongo_utils import init_mongodb
        mongo_success = init_mongodb()
    except Exception as e:
        print(f"❌ MongoDB initialization failed: {e}")
        mongo_success = False

    if pg_success and mongo_success:
        print("\n✅ All databases initialized successfully!")
        return True
    else:
        print("\n⚠️ Some databases failed to initialize")
        return False


if __name__ == "__main__":
    init_all_databases()