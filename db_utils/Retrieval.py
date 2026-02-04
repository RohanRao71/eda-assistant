#Retrieval.py
import pandas as pd
from datetime import date
from sqlalchemy import create_engine, Column, Integer, String, Text,DateTime, insert,text
import json

def get_dataset_metadata(dataset_id,engine):
    """
    Fetch metadata for a given dataset_id.
    Returns a dict or None if dataset does not exist.
    """

    query = text("""
          SELECT
              dataset_id,
              dataset_name,
              table_name,
              num_rows,
              num_columns,
              upload_date,
              owner_user_id,
              file_path,
              column_names
          FROM datasets_metadata
          WHERE dataset_id = :dataset_id;
      """)

    try:
        with engine.connect() as conn:
            print("Connected (transaction started)")
            result = conn.execute(query, {"dataset_id": dataset_id})
            row = result.fetchone()

            return {
                "dataset_id": row.dataset_id,
                "dataset_name": row.dataset_name,
                "table_name": row.table_name,
                "num_rows": row.num_rows,
                "num_columns": row.num_columns,
                "upload_date": row.upload_date,
                "owner_user_id": row.owner_user_id,
                "column_names":row.column_names,
                "file_name":row.file_path
            }

    except Exception as e:
        raise RuntimeError(f"Error in fetching data {e}")

def get_column_details(dataset_id,engine):

    query = text("""
        SELECT
            column_name,
            pandas_dtype,
            column_type,

            mean,
            median,
            std_dev,
            min_value,
            max_value,

            missing_values,
            unique_value_count,

            distinct_categories,
            min_datetime,
            max_datetime
        FROM dataset_column_details
        WHERE dataset_id = :dataset_id
        ORDER BY column_name;
    """)

    try:
        with engine.connect() as conn:
            result = conn.execute(query, {"dataset_id": dataset_id})
            rows = result.fetchall()

            if not rows:
                return []

            columns = []
            for row in rows:
                columns.append({
                    "column_name": f'"{row.column_name}"',
                    "pandas_dtype": row.pandas_dtype,
                    "column_type": row.column_type,

                    "mean": row.mean,
                    "median": row.median,
                    "std_dev": row.std_dev,
                    "min_value": row.min_value,
                    "max_value": row.max_value,

                    "missing_values": row.missing_values,
                    "unique_value_count": row.unique_value_count,

                    "distinct_categories": row.distinct_categories,
                    "min_datetime": row.min_datetime,
                    "max_datetime": row.max_datetime
                })

            return columns

    except Exception as e:
        raise RuntimeError(f"Failed to fetch column details: {e}")

def get_table_name(dataset_id,engine):

    query = text("""
    SELECT dataset_id, table_name 
    FROM datasets_metadata
    WHERE dataset_id = :dataset_id;""")

    try:
        with engine.begin() as conn:
            result = conn.execute(query,{"dataset_id":dataset_id})
            record = result.fetchone()
            table_name = record.table_name

            return table_name
    except Exception as e:
        raise RuntimeError(f"Failed to fetch column details: {e}")

def normalize_column_name(col: str) -> str:
    if not isinstance(col, str):
        raise ValueError(f"Invalid column type: {type(col)}")

    col = col.strip()

    # Strip wrapping quotes repeatedly
    while (
        (col.startswith('"') and col.endswith('"')) or
        (col.startswith("'") and col.endswith("'"))
    ):
        col = col[1:-1].strip()

    return col

def quote_identifier(col: str) -> str:
    # Escape embedded quotes for PostgreSQL
    escaped = col.replace('"', '""')
    return f'"{escaped}"'

import re

def normalize_where_clause(where_clause: str) -> str:
    if where_clause is None:
        return None

    # Replace identifier-quoted literals with string literals
    # "gender" = "female" → "gender" = 'female'
    where_clause = re.sub(
        r'=\s*"([^"]+)"',
        r"= '\1'",
        where_clause
    )

    # Handle IN clauses: IN ("a","b") → IN ('a','b')
    where_clause = re.sub(
        r'IN\s*\(([^)]+)\)',
        lambda m: "IN (" + re.sub(r'"([^"]+)"', r"'\1'", m.group(1)) + ")",
        where_clause,
        flags=re.IGNORECASE
    )

    return where_clause

def normalize_columns(columns):
    if columns is None:
        return None
    return [col.strip().strip('"') for col in columns]
def get_dataframe(dataset_id, engine, limit=100, columns=None, where_clause=None):

    where_clause = normalize_where_clause(where_clause)
    columns = normalize_columns(columns)

    table_name = get_table_name(dataset_id, engine)
    if table_name is None:
        raise ValueError("Dataset not found")

    # Normalize + quote columns
    if columns:
        clean_columns = [normalize_column_name(c) for c in columns]
        select_cols = ", ".join(quote_identifier(c) for c in clean_columns)
    else:
        select_cols = "*"

    sql = f"SELECT {select_cols} FROM {table_name}"

    if where_clause:
        sql += f" WHERE {where_clause}"

    sql += f" LIMIT {int(limit)}"

    try:
        with engine.connect() as conn:
            return pd.read_sql(text(sql), conn)
    except Exception as e:
        raise RuntimeError(f"SQL execution failed: {e}")
