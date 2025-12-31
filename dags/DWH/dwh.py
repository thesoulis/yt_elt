from DWH.data_utilis import get_conn_cursor, close_conn_cursor, create_schema, create_table, get_video_ids
from DWH.data_loading import load_data

from DWH.data_modification import insert_rows, update_rows, delete_rows
from DWH.data_transform import transform_data

import logging
from airflow.decorators import task

logger = logging.getLogger(__name__)
table = 'yt_api'

@task
def staging_table():
    schema = 'staging'
    con, cur = None, None

    try:
        con, cur = get_conn_cursor()    
        YT_data = load_data()
        create_schema(schema)
        create_table(schema)
        table_ids= get_video_ids(cur,schema)

        for row in YT_data:

            if len(table_ids)==0:
                insert_rows(cur,con,schema,row)

            else:
                if row['video_id'] not in table_ids:
                    insert_rows(cur,con,schema,row)
                else:
                    update_rows(cur,con,schema,row)

        ids_in_json= {row['video_id'] for row in YT_data    
                      }
        
        ids_to_delete= set(table_ids) - ids_in_json

        if ids_to_delete:
            delete_rows(cur,con,schema,ids_to_delete)

        logger.info("Staging table ETL process completed successfully.")

    except Exception as e:
        logger.error("Error in staging table ETL process.")
        raise e
    
    finally:
        if con and cur:
            close_conn_cursor(con, cur)

    
@task

def core_table():
    schema = 'core'
    con, cur = None, None

    try:
        con, cur = get_conn_cursor()
        create_schema(schema)
        create_table(schema)

        table_ids= get_video_ids(cur,schema)

        current_video_ids= set()
        cur.execute(f"SELECT * FROM staging.{table};")
        rows=cur.fetchall()

        for row in rows:
            current_video_ids.add(row['video_id'])

            if len(table_ids)==0:
                transformed_row= transform_data(row)
                insert_rows(cur,con,schema,transformed_row)
            
            else:
                transformed_row= transform_data(row)
                if transformed_row['video_id'] in table_ids:
                    update_rows(cur,con,schema,transformed_row)
                else:
                    insert_rows(cur,con,schema,transformed_row)
        
        ids_to_delete= set(table_ids) - current_video_ids

        if  ids_to_delete:
            delete_rows(cur,con,schema,ids_to_delete)
        
        logger.info("Core table ETL process completed successfully.")
    
    except Exception as e:
        logger.error("Error in core table ETL process.")
        raise e
    finally:
        if con and cur:
            close_conn_cursor(con, cur)

