# Utilities


import datetime

from dateutil.relativedelta import relativedelta, MO

epoch = datetime.date(1970, 1, 1)


def get_last_day(dt):
    """Gets date of current day, or last day if before 5 AM

    If a conversation occurs in the early morning, it is likely
    a continuation of one from a prior day
    :param dt: a datetime or datetime-like to convert
    :return: a date object
    """
    if dt.hour < 5:
        return dt.date() + dt.timedelta(days=-1)
    else:
        return dt.date()


def get_last_monday(dt):
    """Gets the date of the previous monday"""
    return dt.date() + relativedelta(weekday=MO(-1))


def get_day_number(dt):
    """Gets days since the epoch 1/1/1970"""
    return (dt - epoch).days
