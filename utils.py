import json
import logging
from datetime import datetime


def load_config():
    """Load configuration from config.json."""
    try:
        with open('config.json') as config_file:
            return json.load(config_file)
    except FileNotFoundError:
        logging.error("Configuration file not found.")
        raise


def format_timestamp(timestamp):
    """Format timestamps to BigQuery-compatible format."""
    if not timestamp or isinstance(timestamp, bool):
        return None
    try:
        dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S.%f')
        return dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    except ValueError:
        try:
            dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
            return dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        except ValueError:
            try:
                dt = datetime.strptime(timestamp, '%Y-%m-%d')
                return dt.strftime('%Y-%m-%d')
            except ValueError as ve:
                return str(timestamp)


def safe_get(data, field):
    """Helper function to safely get field values."""
    return data[field] if field in data and data[field] is not None else None
