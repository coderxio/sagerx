from datetime import datetime, date, timedelta
import calendar


def get_date_of_prior_weekday(
    weekday, reference_date=date.today(), date_format="%m-%d-%Y"
):

    """Gets the date of the prior weekday.  If today is the same weekday, returns today's date."""
    weekday = weekday.lower()
    weekdays = [d.lower() for d in list(calendar.day_name)]
    weekday_number = weekdays.index(weekday)

    if weekday_number < 0:
        raise ValueError

    offset = (reference_date.weekday() - weekday_number) % 7
    prior_weekday = reference_date - timedelta(days=offset)
    prior_weekday = prior_weekday.strftime(date_format)

    return prior_weekday


def ds_datetime(ds):
    return datetime.strptime(ds, "%Y-%m-%d")
