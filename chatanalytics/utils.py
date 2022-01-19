# Utilities


import datetime

from dateutil.relativedelta import relativedelta, MO

epoch = datetime.date(1970, 1, 1)


def get_last_day(dt):
    """Gets date of current day

    :param dt: a datetime or datetime-like to convert
    :return: a date object
    """
    return dt.date()


def get_day_of_messages(messages):
    days = messages.apply(lambda row: get_last_day(row["timestamp"]), axis=1)
    return messages.assign(day=days)


def get_last_waking_day(dt):
    """Gets date of "waking" day, or last day if before 5 AM

    If a conversation occurs in the early morning, it is likely
    a continuation of one from a prior day
    :param dt: a datetime or datetime-like to convert
    :return: a date object
    """
    if dt.hour < 5:
        return dt.date() + dt.timedelta(days=-1)
    else:
        return dt.date()


def get_waking_day_of_messages(messages):
    days = messages.apply(lambda row: get_last_waking_day(row["timestamp"]), axis=1)
    return messages.assign(waking_day=days)


def get_last_monday(dt):
    """Gets the date of the previous monday"""
    return dt.date() + relativedelta(weekday=MO(-1))


def get_week_of_messages(messages):
    weeks = messages.apply(lambda row: get_last_monday(row["timestamp"]), axis=1)
    return messages.assign(week=weeks)


def get_first_day_of_month(dt):
    """Gets the date of the previous monday"""
    return dt.date().replace(day=1)


def get_month_of_messages(messages):
    months = messages.apply(lambda row: get_first_day_of_month(row["timestamp"]), axis=1)
    return messages.assign(month=months)


def get_first_day_of_year(dt):
    """Gets the date of the previous monday"""
    return dt.date().replace(day=1, month=1)


def get_year_of_messages(messages):
    years = messages.apply(lambda row: get_first_day_of_year(row["timestamp"]), axis=1)
    return messages.assign(year=years)


def get_day_number(dt):
    """Gets days since the epoch 1/1/1970"""
    return (dt - epoch).days
