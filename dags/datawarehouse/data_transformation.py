from datetime import timedelta, datetime


def parse_duration(duration_str):
    """
    Parse ISO 8601 duration format (e.g., "PT30M13S") to timedelta
    """
    duration_str = duration_str.replace("P", "").replace("T", "")

    components = ["D", "H", "M", "S"]
    values = {"D": 0, "H": 0, "M": 0, "S": 0}

    for component in components:
        if component in duration_str:
            value, duration_str = duration_str.split(component)
            values[component] = int(value)

    total_duration = timedelta(
        days=values["D"], hours=values["H"], minutes=values["M"], seconds=values["S"]
    )

    return total_duration


def transform_data(row):
    """
    Transform staging data for core table
    Converts duration from string to time object and adds video type
    
    Args:
        row: Dictionary from staging table with keys:
            - video_id
            - video_title
            - upload_date
            - duration (string in ISO 8601 format)
            - video_views
            - likes_count
            - comments_count
    
    Returns:
        Dictionary with transformed data for core table
    """
    # Parse duration from string to timedelta
    duration_td = parse_duration(row["duration"])
    
    # Create transformed dictionary with lowercase field names
    transformed_row = {
        "video_id": row["video_id"],
        "video_title": row["video_title"],
        "upload_date": row["upload_date"],
        "duration": (datetime.min + duration_td).time(),
        "video_type": "Shorts" if duration_td.total_seconds() <= 60 else "Normal",
        "video_views": row["video_views"],
        "likes_count": row["likes_count"],
        "comments_count": row["comments_count"]
    }
    
    return transformed_row