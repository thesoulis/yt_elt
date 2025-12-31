import logging 

logger=logging.getLogger(__name__)
table='yt_api'

def insert_rows(cur,conn,schema,row):

    try:

        if schema == 'staging':
            video_id= 'video_id'

            cur.execute(f"""
            INSERT INTO {schema}.{table} (video_id, video_title, upload_date, duration, video_views, video_likes, comment_count)
            VALUES (%(video_id)s, %(title)s, %(publishedAt)s, %(duration)s, %(viewCount)s, %(likeCount)s, %(commentCount)s)
            """,row)

        else:
            video_id= 'video_id'

            cur.execute(f"""
            INSERT INTO {schema}.{table} (video_id, video_title, upload_date, duration, video_type, video_views, video_likes, comment_count)
            VALUES (%(video_id)s, %(title)s, %(publishedAt)s, %(duration)s, %(video_type)s, %(viewCount)s, %(likeCount)s, %(commentCount)s)
            """,row)
        
        conn.commit()

        logger.info(f"Inserted video_id {row[video_id]} into {schema}.{table}")
    
    except Exception as e:
        logger.error(f"Error inserting video_id {row[video_id]} ")
        raise e




def update_rows(cur,conn,schema,row):

    try:

        if schema == 'staging':
            video_id= 'video_id'
            upload_date='upload_date'
            video_title='video_title'
            video_views='video_views'
            video_likes='video_likes'
            comment_count='comment_count'

        else:
            video_id= 'video_id'
            upload_date='upload_date'
            video_title='video_title'
            video_views='video_views'
            video_likes='video_likes'
            comment_count='comment_count'
 

            cur.execute(
                f"""
                UPDATE {schema}.{table}
                SET "video_title"=%({video_title})s,
                    "video_views"=%({video_views})s,
                    "video_likes"=%({video_likes})s,
                    "comment_count"=%({comment_count})s
                WHERE "video_id"=%({video_id})s AND "upload_date"=%({upload_date})s;
                """,row)
        
        conn.commit()

        logger.info(f"Updated video_id {row[video_id]} in {schema}.{table}")
    
    except Exception as e:
        logger.error(f"Error updating video_id {row[video_id]} ")
        raise e


def delete_rows(cur,conn,schema,ids_to_delete):

    try:    

        ids_to_delete =f"""{','.join(f"'{id}'" for id in ids_to_delete)}"""
    
        cur.execute(
            f"""
            DELETE FROM {schema}.{table}
            WHERE video_id IN ({ids_to_delete});
            """)
        conn.commit()
        logger.info(f"Deleted video_ids {ids_to_delete} from {schema}.{table}")

    except Exception as e:
        logger.error(f"Error deleting video_ids {ids_to_delete} - {e}")
        raise e