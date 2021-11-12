from datetime import datetime, date, timedelta
import calendar


def ds_datetime(ds):
    return datetime.strptime(ds, "%Y-%m-%d")
