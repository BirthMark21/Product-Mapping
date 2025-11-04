# pipeline/db_writer.py

import pandas as pd
from sqlalchemy import create_engine

def write_to_db(df: pd.DataFrame, engine, table_name: str, schema: str, if_exists: str = 'replace'):
    """
    Writes a pandas DataFrame to a specified database table.

    Args:
        df (pd.DataFrame): The DataFrame to write.
        engine: The SQLAlchemy engine for the destination database.
        table_name (str): The name of the target table.
        schema (str): The database schema for the target table.
        if_exists (str): How to behave if the table already exists.
                         'replace', 'append', or 'fail'.
    """

    if df.empty:
        print(f" -> DataFrame is empty. No data to write to '{schema}.{table_name}'.")
        return

    print(f"Writing {len(df)} records to table '{schema}.{table_name}' (mode: {if_exists})...")

    try:
        # Use pandas' to_sql function to write the data
        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists=if_exists,
            index=False  # Do not write the DataFrame index as a column
        )
        print("SUCCESS: Successfully wrote data to the database.")

    except Exception as e:
        print(f"ERROR: Failed to write data to the database. Error: {e}")
        raise