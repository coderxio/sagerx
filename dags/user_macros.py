from datetime import date, timedelta
import calendar


def get_date_of_prior_weekday(weekday, start_date=date.today(), date_format="%m-%d-%Y"):
    """Gets the date of the prior weekday.  If today is the same weekday, returns today's date."""
    weekday = weekday.lower()
    weekdays = [d.lower() for d in list(calendar.day_name)]
    weekday_number = weekdays.index(weekday)

    if weekday_number < 0:
        raise ValueError

    offset = (start_date.weekday() - weekday_number) % 7
    prior_weekday = start_date - timedelta(days=offset)
    prior_weekday = prior_weekday.strftime(date_format)

    return prior_weekday
