import pandas as pd
from datetime import datetime


def standardize_date(date_str):
    """
    Standardizes various date string formats to 'YYYY-MM-DD'.
    Returns None if the date string is invalid.

    Args:
        date_str (str): The date string to standardize.

    Returns:
        The standardized date string in 'YYYY-MM-DD' format, or None.
    """

    if pd.isna(date_str) or not isinstance(date_str, str):
        return None

    date_str = date_str.strip()

    # parse common date formats (needs generalisation if we opt for a production environement)
    try:
        return datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
    except ValueError:
        try:
            return datetime.strptime(date_str, "%d %B %Y").strftime("%Y-%m-%d")
        except ValueError:
            try:
                return datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y-%m-%d")
            except ValueError:
                return None
