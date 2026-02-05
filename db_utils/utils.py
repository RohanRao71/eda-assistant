#utils.py
from db_utils.Ingestion import ingest_dataset
from db_utils.Retrieval import get_dataframe,get_column_details,get_dataset_metadata
import bcrypt
from sqlalchemy.exc import IntegrityError
import streamlit as st

import traceback
import logging

logging.basicConfig(
    filename="../app_errors.log",
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def handle_error(user_message="❌ Something went wrong. Please try again."):
    err = traceback.format_exc()
    logging.error(err)
    st.error(user_message)

def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode(),bcrypt.gensalt()).decode()

def verify_password(password: str, password_hash:str)->bool:
    return bcrypt.checkpw(password.encode(),password_hash.encode())

def register_user(username, password, engine):


    try:
        with engine.connect() as conn:
            exists = conn.execute(
                text("SELECT 1 FROM user_details WHERE username = :u"),
                {"u": username}
            ).fetchone()

        if exists:
            return {"success": False, "error": "Username already exists"}

        password_hash = hash_password(password)

        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO user_details (username, password_hash)
                    VALUES (:username, :password_hash);
                """),
                {"username": username, "password_hash": password_hash}
            )

        return {"success": True}

    except IntegrityError:
        # Absolute safety net
        return {"success": False, "error": "Username already exists"}


def authenticate_user(username, password, engine):
    with engine.connect() as conn:
        row = conn.execute(
            text("""
                SELECT user_id, password_hash
                FROM user_details
                WHERE username = :username;
            """),
            {"username": username}
        ).fetchone()

        if row is None:
            return {"success": False, "error": "Invalid username or password"}

        if not verify_password(password, row.password_hash):
            return {"success": False, "error": "Invalid username or password"}

        # Update last_active
        conn.execute(
            text("""
                UPDATE user_details
                SET last_active = now()
                WHERE user_id = :user_id;
            """),
            {"user_id": row.user_id}
        )

        return {
            "success": True,
            "user_id": row.user_id
        }

def create_new_project(user_id,project_name,description,engine):

    query = text("""INSERT into projects(project_name,description,owner_user_id)
    VALUES(:project_name,:description,:user_id) RETURNING project_id;""")

    try:
        with engine.begin() as conn:
            inputs = {
                "project_name":project_name,
                "description":description,
                "user_id":user_id
            }
            result = conn.execute(query,inputs)
            project_id = result.fetchone()[0]

        return {"success":True,
                "project_id":project_id}
    except Exception as e:
        return {
            "success":False,
            "error": str(e)
        }

def upload_dataset_to_project(project_id, csv_path, original_filename, user_id, engine):
    """
    Upload a dataset and attach it to an existing project.
    Enforces one-dataset-per-project rule.
    """

    try:
        with engine.begin() as conn:

            project = conn.execute(
                text("""
                    SELECT project_id, dataset_id
                    FROM projects
                    WHERE project_id = :project_id
                      AND owner_user_id = :user_id;
                """),
                {"project_id": project_id, "user_id": user_id}
            ).fetchone()

            if project is None:
                return {
                    "success": False,
                    "error": "Project not found or access denied"
                }

            if project.dataset_id is not None:
                return {
                    "success": False,
                    "error": "Project already has a dataset"
                }

            # Ingest dataset
            ingestion_result = ingest_dataset(csv_path, original_filename, user_id, engine)

            if not ingestion_result["success"]:
                return ingestion_result

            dataset_id = ingestion_result["dataset_id"]

            # Attach dataset to project
            conn.execute(
                text("""
                    UPDATE projects
                    SET dataset_id = :dataset_id
                    WHERE project_id = :project_id;
                """),
                {
                    "dataset_id": dataset_id,
                    "project_id": project_id
                }
            )

        return {
            "success": True,
            "project_id": project_id,
            "dataset_id": dataset_id
        }

    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

def list_projects(user_id,engine):

    query = text("""
            SELECT project_id,project_name,description,dataset_id,created_at
            FROM projects
            WHERE owner_user_id=:user_id;""")

    project_list = []

    try:
        with engine.connect() as conn:
            result = conn.execute(query,{"user_id":user_id})
            rows = result.fetchall()
            if not rows:
                return {
                    "success": True,
                    "project_list": None
                }

            for row in rows:
                project_list.append({
                    "project_id":row.project_id,
                    "project_name":row.project_name,
                    "description":row.description,
                    "dataset_id":row.dataset_id,
                    "created_at":row.created_at
                })

            return {
                "success":True,
                "project_list":project_list
            }

    except Exception as e:
        return {
            "success" : False,
            "error" : str(e)
        }
def delete_project(project_id, user_id, engine):
    """
    Delete a project and its associated dataset (if any).
    Fully transactional and ownership-protected.
    """

    try:
        with engine.begin() as conn:

            # 1. Verify project ownership and fetch dataset_id
            project_row = conn.execute(
                text("""
                    SELECT project_id, dataset_id
                    FROM projects
                    WHERE project_id = :project_id
                      AND owner_user_id = :user_id;
                """),
                {"project_id": project_id, "user_id": user_id}
            ).fetchone()

            if project_row is None:
                return {
                    "success": False,
                    "error": "Project not found or access denied"
                }

            dataset_id = project_row.dataset_id

            # 2. If dataset exists, delete dataset-related artifacts
            if dataset_id is not None:

                # a) Drop dynamically created dataset table
                conn.execute(
                    text(f'DROP TABLE IF EXISTS dataset_{dataset_id}_data;')
                )

                # b) Delete column metadata
                conn.execute(
                    text("""
                        DELETE FROM dataset_column_details
                        WHERE dataset_id = :dataset_id;
                    """),
                    {"dataset_id": dataset_id}
                )

                # c) Delete dataset metadata
                conn.execute(
                    text("""
                        DELETE FROM datasets_metadata
                        WHERE dataset_id = :dataset_id;
                    """),
                    {"dataset_id": dataset_id}
                )

            # 3. Delete project itself
            conn.execute(
                text("""
                    DELETE FROM projects
                    WHERE project_id = :project_id;
                """),
                {"project_id": project_id}
            )

        return {
            "success": True,
            "project_id": project_id,
            "dataset_deleted": dataset_id is not None
        }

    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

from sqlalchemy import text

def unlink_dataset_from_project(project_id, user_id, engine):
    """
    Delete the dataset linked to a project.
    The project remains, dataset_id is set to NULL via FK (ON DELETE SET NULL).
    """

    try:
        with engine.begin() as conn:

            # 1. Verify project ownership and get dataset_id
            row = conn.execute(
                text("""
                    SELECT dataset_id
                    FROM projects
                    WHERE project_id = :project_id
                      AND owner_user_id = :user_id;
                """),
                {"project_id": project_id, "user_id": user_id}
            ).fetchone()

            if row is None:
                return {
                    "success": False,
                    "error": "Project not found or access denied"
                }

            if row.dataset_id is None:
                return {
                    "success": False,
                    "error": "Project has no dataset linked"
                }

            dataset_id = row.dataset_id

            # 2. Drop dataset data table
            conn.execute(
                text(f'DROP TABLE IF EXISTS dataset_{dataset_id}_data;')
            )

            # 3. Delete column metadata
            conn.execute(
                text("""
                    DELETE FROM dataset_column_details
                    WHERE dataset_id = :dataset_id;
                """),
                {"dataset_id": dataset_id}
            )

            # 4. Delete dataset metadata
            conn.execute(
                text("""
                    DELETE FROM datasets_metadata
                    WHERE dataset_id = :dataset_id;
                """),
                {"dataset_id": dataset_id}
            )
            # FK ON DELETE SET NULL updates projects.dataset_id automatically

        return {
            "success": True,
            "project_id": project_id,
            "deleted_dataset_id": dataset_id
        }

    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


def get_project_metadata(project_id, engine):
    try:
        with engine.connect() as conn:
            project_row = conn.execute(
                text("""
                    SELECT project_id, project_name, description, created_at, dataset_id
                    FROM projects
                    WHERE project_id = :project_id;
                """),
                {"project_id": project_id}
            ).fetchone()

            if project_row is None:
                return {
                    "success": False,
                    "error": "Project not found"
                }

            project_info = {
                "project_id": project_row.project_id,
                "project_name": project_row.project_name,
                "description": project_row.description,
                "created_at": project_row.created_at,
                "has_dataset": project_row.dataset_id is not None,
                "dataset": None
            }

            if project_row.dataset_id is not None:
                dataset_meta = get_dataset_metadata(project_row.dataset_id, engine)

                project_info["dataset"] = dataset_meta

            return {
                "success": True,
                "project": project_info
            }

    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }



def get_project_stats(project_id, engine):
    """
    Fetch column-level statistics for a project's dataset.
    """

    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT dataset_id
                    FROM projects
                    WHERE project_id = :project_id;
                """),
                {"project_id": project_id}
            )

            row = result.fetchone()

            if row is None:
                return {
                    "success": False,
                    "error": "Project not found"
                }

            if row.dataset_id is None:
                return {
                    "success": False,
                    "error": "Project has no dataset linked"
                }

            dataset_id = row.dataset_id

        stats = get_column_details(dataset_id, engine)

        return {
            "success": True,
            "stats": stats
        }

    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

def normalize_columns(columns):
    if columns is None:
        return None
    return [col.strip().strip('"') for col in columns]

def get_project_data(project_id,engine,columns = None,limit = 100,where_clause = None):

    columns = normalize_columns(columns)

    project_meta = get_project_metadata(project_id,engine)

    if not project_meta["success"]:
        return{
            "success":False,
            "error":project_meta["error"]
        }
    project = project_meta["project"]

    if not project["has_dataset"]:
        return {
            "success":False,
            "error":"Project does not have a dataset uploaded"
        }

    dataset_id = project["dataset"]["dataset_id"]

    try:
        df = get_dataframe(dataset_id,engine,limit,columns,where_clause)

        return{
            "success":True,
            "data":df
        }
    except Exception as e:
        return {
            "success": False,
            "error":str(e)
        }