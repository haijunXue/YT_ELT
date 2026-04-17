import logging

logger = logging.getLogger(__name__)
table = "yt_api"


def insert_rows(cur, conn, schema, row):
    try:
        if schema == "staging":
            cur.execute(
                f"""
                INSERT INTO {schema}.{table}(video_id, video_title, upload_date, duration, video_views, likes_count, comments_count)
                VALUES (%(videoId)s, %(title)s, %(publishedAt)s, %(duration)s, %(viewCount)s, %(likeCount)s, %(commentCount)s);
                """,
                row,
            )
            logger.info(f"Inserted row with video_id: {row['videoId']}")
        else:
            cur.execute(
                f"""
                INSERT INTO {schema}.{table}(video_id, video_title, upload_date, duration, video_type, video_views, likes_count, comments_count)
                VALUES (%(video_id)s, %(video_title)s, %(upload_date)s, %(duration)s, %(video_type)s, %(video_views)s, %(likes_count)s, %(comments_count)s)
                """,
                row,
            )
            logger.info(f"Inserted row with video_id: {row['video_id']}")
        
        conn.commit()
    except Exception as e:
        logger.error(f"Error inserting row: {e}")
        raise e


def update_rows(cur, conn, schema, row):
    try:
        if schema == "staging":
            cur.execute(
                f"""
                UPDATE {schema}.{table}
                SET video_title = %(title)s,
                    video_views = %(viewCount)s, 
                    likes_count = %(likeCount)s, 
                    comments_count = %(commentCount)s
                WHERE video_id = %(videoId)s AND upload_date = %(publishedAt)s;
                """,
                row,
            )
            logger.info(f"Updated row with video_id: {row['videoId']}")
        else:
            cur.execute(
                f"""
                UPDATE {schema}.{table}
                SET video_title = %(video_title)s,
                    video_views = %(video_views)s, 
                    likes_count = %(likes_count)s, 
                    comments_count = %(comments_count)s
                WHERE video_id = %(video_id)s AND upload_date = %(upload_date)s;
                """,
                row,
            )
            logger.info(f"Updated row with video_id: {row['video_id']}")
        
        conn.commit()
    except Exception as e:
        logger.error(f"Error updating row: {e}")
        raise e


def delete_rows(cur, conn, schema, ids_to_delete):
    try:
        ids_formatted = f"""({', '.join(f"'{id}'" for id in ids_to_delete)})"""
        cur.execute(
            f"""
            DELETE FROM {schema}.{table}
            WHERE video_id IN {ids_formatted};
            """
        )
        conn.commit()
        logger.info(f"Deleted rows with video_ids: {ids_formatted}")
    except Exception as e:
        logger.error(f"Error deleting rows: {e}")
        raise e