

import logging
from airflow.decorators import task
from airflow.operators.python import get_current_context

from datawarehouse.data_utils import get_conn_cursor, close_conn_cursor, create_schema, create_table, get_video_ids
from datawarehouse.data_modification import insert_rows, update_rows, delete_rows
from datawarehouse.data_transformation import transform_data
from datawarehouse.data_loading import load_data

logger = logging.getLogger(__name__)
table = "yt_api"


@task
def staging_table():
    """
    Task to update the staging schema table with raw YouTube API data.
    Performs incremental updates: inserts new records, updates existing ones,
    and deletes records that no longer exist in the source JSON.
    """
    schema = "staging"
    conn, cur = None, None

    try:
        # Get the execution date from Airflow context
        context = get_current_context()
        execution_date = context.get("execution_date", None)

        # Establish database connection
        conn, cur = get_conn_cursor()

        # Load data for the specific execution date - pass execution_date explicitly
        YT_data = load_data(execution_date=execution_date)

        # Create schema and table if they don't exist
        create_schema(schema)
        create_table(schema)

        # Get existing video IDs from the database
        table_ids = get_video_ids(cur, schema)

        # Process each row from the JSON data
        for row in YT_data:
            if not table_ids:
                # Table is empty, insert all rows
                insert_rows(cur, conn, schema, row)
            elif row["videoId"] in table_ids:
                # Video exists, update it
                update_rows(cur, conn, schema, row)
            else:
                # New video, insert it
                insert_rows(cur, conn, schema, row)

        # Delete records that are in the database but not in the JSON
        ids_in_json = {row["videoId"] for row in YT_data}
        ids_to_delete = set(table_ids) - ids_in_json
        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)

        logger.info(f"{schema} table update completed")

    except Exception as e:
        logger.error(f"An error occurred during the update of {schema} table: {e}")
        raise
    finally:
        # Always close database connection
        if conn and cur:
            close_conn_cursor(conn, cur)


@task
def core_table():
    """
    Task to update the core schema table with transformed data from staging.
    Applies data transformations and performs incremental updates.
    """
    schema = "core"
    conn, cur = None, None

    try:
        # Establish database connection
        conn, cur = get_conn_cursor()
        
        # Create schema and table if they don't exist
        create_schema(schema)
        create_table(schema)

        # Get existing video IDs from the core table
        table_ids = get_video_ids(cur, schema)
        current_video_ids = set()

        # Fetch all rows from staging table
        cur.execute(f"SELECT * FROM staging.{table};")
        rows = cur.fetchall()

        # Process each row from staging
        for row in rows:
            current_video_ids.add(row["video_id"])
            
            # Transform the data for core table
            transformed_row = transform_data(row)

            # Insert or update based on existence
            if not table_ids:
                insert_rows(cur, conn, schema, transformed_row)
            elif transformed_row["video_id"] in table_ids:
                update_rows(cur, conn, schema, transformed_row)
            else:
                insert_rows(cur, conn, schema, transformed_row)

        # Delete records that are no longer in staging
        ids_to_delete = set(table_ids) - current_video_ids
        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)

        logger.info(f"{schema} table update completed")

    except Exception as e:
        logger.error(f"An error occurred during the update of {schema} table: {e}")
        raise
    finally:
        # Always close database connection
        if conn and cur:
            close_conn_cursor(conn, cur)
