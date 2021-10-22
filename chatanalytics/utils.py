# Utilities


import datetime

from dateutil.relativedelta import relativedelta, MO

epoch = datetime.date(1970, 1, 1)


def label_days(df):
    df = df.assign(day=lambda x: (x["timestamp"]))


def get_last_day(date):
    if date.hour < 5:
        return date.date() + datetime.timedelta(days=-1)
    else:
        return date.date()


def get_last_monday(date):
    return date.date() + relativedelta(weekday=MO(-1))


def get_day_number(dt):
    return (dt - epoch).days
