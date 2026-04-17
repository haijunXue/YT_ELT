# data_loading.py
import json
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def load_data(execution_date=None):
    """
    Load JSON data for a specific date.
    
    Args:
        execution_date: datetime or None -> uses today's date if None
    
    Returns:
        dict: JSON data loaded from the file
    """
    # Determine the file date based on execution_date parameter
    if execution_date is None:
        file_date = datetime.today().date()
    else:
        # Airflow passes Pendulum datetime, extract date part
        file_date = getattr(execution_date, "date", lambda: execution_date)()
    
    # Construct the file path using the determined date
    file_path = f"./data/YT_data_{file_date}.json"
    
    try:
        logger.info(f"Processing file: {file_path}")
        with open(file_path, "r", encoding="utf-8") as raw_data:
            data = json.load(raw_data)
        return data
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON in file: {file_path}")
        raise