#Ingestion.py
import pandas as pd
from datetime import date
from sqlalchemy import create_engine, Column, Integer, String, Text,DateTime, insert,text
import json

import re
import os


def make_param_name(col):
    return re.sub(r'\W+', '_', col)
def map_dtype_to_sqltype(dtype):
    if "int" in str(dtype):
        return "INTEGER"
    elif "float" in str(dtype):
        return "FLOAT"
    elif "bool" in str(dtype):
        return "BOOLEAN"
    elif "datetime" in str(dtype):
        return "TIMESTAMP"
    else:
        return "TEXT"
def ingest_dataset(csv_file_path, original_filename, user_id,engine):
    """Ingest a CSV into:
        - datasets_metadata
        - dynamically generated dataset_X_data table
        - dataset_column_details
    Wrapped in a single ACID transaction.
    """

    ingestion_result = {
        "success": False,
        "dataset_id": None,
        "error": None
    }

    # Reading csv file

    df = pd.read_csv(csv_file_path)

    dataset_metadata_table = "datasets_metadata"

    # Extracting important Information
    df_name = os.path.splitext(original_filename)[0]
    num_rows = df.shape[0]
    num_cols = df.shape[1]
    upload_date = date.today()
    file_path = original_filename
    column_names = df.columns.tolist()

    dataset_metadata = {
        "dataset_name": df_name,
        "file_path": file_path,
        "upload_date": upload_date,
        "num_rows": num_rows,
        "num_columns": num_cols,
        "owner_user_id": user_id,
        "column_names":column_names
    }

# Postgres connection and inserting into datasets_metadata table

    try:
        with engine.begin() as conn:

            print("Connected (transaction started)")

            # Inserting into datasets_metadata

            result = conn.execute(
                text("""
                    INSERT INTO datasets_metadata 
                    (dataset_name, file_path, upload_date, num_rows,     num_columns, owner_user_id, column_names)
                    VALUES 
                    (:dataset_name, :file_path, :upload_date, :num_rows, :num_columns, :owner_user_id, :column_names)
                    RETURNING dataset_id;
                """),
                {
                    "dataset_name": dataset_metadata["dataset_name"],
                    "file_path": dataset_metadata["file_path"],
                    "upload_date": dataset_metadata["upload_date"],
                    "num_rows": dataset_metadata["num_rows"],
                    "num_columns": dataset_metadata["num_columns"],
                    "owner_user_id": dataset_metadata["owner_user_id"],
                    "column_names":dataset_metadata["column_names"]
                }
            )

            # Retrieving dataset_id and creating table name for dataset table

            dataset_id = result.fetchone()[0]

            ingestion_result["dataset_id"] = dataset_id # Storing current dataset_id in global ingestion_result

            print("Inserted dataset_id:", dataset_id)

            # Build table_name based on dataset_id
            table_name = f"dataset_{dataset_id}_data"

            # Creating dataset table

            conn.execute(
                text("""
                            UPDATE datasets_metadata
                            SET table_name = :table_name
                            WHERE dataset_id = :dataset_id;
                        """),
                {"table_name": table_name, "dataset_id": dataset_id}
            )

            print("Updated table name: ",table_name)

            column_definitions = []

            # Creating sql table column names based on dataset columns

            for col in df.columns:
                sql_type = map_dtype_to_sqltype(df[col].dtype)
                safe_col = f'"{col}"'  # Quote column names to allow spaces, caps, etc.
                column_definitions.append(f"{safe_col} {sql_type}")

            columns_sql = ", ".join(column_definitions)

            create_table_sql = f"""
                CREATE TABLE {table_name} (
                    {columns_sql}
                );
            """

            conn.execute(text(create_table_sql))

            print(f"Created table: {table_name}")

            # Insert DataFrame rows into table

            df.to_sql(
                table_name,
                conn,
                if_exists="append",
                index=False,
                method="multi"
            )

            print(f"Insertion into {table_name} - complete")

            # Calculating stats based on column type

            for col in df.columns:
                column_name = col
                pandas_dtype = str(df[col].dtype)
                num_missing = int(df[col].isnull().sum())
                unique_values_count = int(df[col].nunique(dropna=True))
                mean = median = std_dev = min_value = max_value = None
                distinct_categories = None
                min_datetime = max_datetime = None

                if pd.api.types.is_numeric_dtype(df[col]):
                    col_type = "Numerical"
                    mean = float(df[col].mean())
                    median = float(df[col].median())
                    std_dev = float(df[col].std())
                    min_value = float(df[col].min(skipna=True))
                    max_value = float(df[col].max(skipna=True))

                    sql = text("""
                        Insert into dataset_column_details (dataset_id,column_name,pandas_dtype,column_type,mean,median,std_dev,min_value,
                        max_value,missing_values,unique_value_count) VALUES(:dataset_id, :column_name, :pandas_dtype, :col_type,
                         :mean, :median, :std_dev, :min_value, :max_value, :num_missing, :unique_value_count);
                         """)

                elif pd.api.types.is_object_dtype(df[col]):
                    col_type = "Categorical"
                    if unique_values_count > 30:
                        distinct_categories = df[col].dropna().unique().tolist()[:30]
                    else:
                        distinct_categories = df[col].dropna().unique().tolist()
                    distinct_categories = json.dumps(distinct_categories)

                    sql = text(""" Insert into dataset_column_details (dataset_id,column_name,pandas_dtype,column_type,
                    missing_values,unique_value_count,distinct_categories) VALUES(:dataset_id, :column_name, :pandas_dtype, :col_type,
                                         :num_missing, :unique_value_count, :distinct_categories);
                                         """)

                elif pd.api.types.is_datetime64_any_dtype(df[col]):
                    col_type = "Datetime"
                    min_datetime = df[col].min()
                    max_datetime = df[col].max()

                    sql = text("""
                                Insert into dataset_column_details (dataset_id,column_name,pandas_dtype,column_type,min_datetime,
                                max_datetime,missing_values,unique_value_count) VALUES(:dataset_id, :column_name, :pandas_dtype, :col_type,
                                 :min_datetime, :max_datetime, :num_missing, :unique_value_count);
                                 """)

                # Creating dataset_column_details table

                conn.execute(sql,
                    {"dataset_id": dataset_id,
                        "column_name": column_name,
                        "pandas_dtype": pandas_dtype,
                        "col_type": col_type,
                        "mean": mean,
                        "median": median,
                        "std_dev": std_dev,
                        "min_value": min_value,
                        "max_value": max_value,
                        "num_missing": num_missing,
                        "unique_value_count": unique_values_count,
                        "distinct_categories":distinct_categories,
                        "min_datetime":min_datetime,
                        "max_datetime":max_datetime

                    }
                )

        ingestion_result["success"] = True

    except Exception as e:
        ingestion_result["error"] = str(e)

    return ingestion_result

#ingestion_output = ingest_dataset(csv_path,userid,engine)

#print(ingestion_output)
