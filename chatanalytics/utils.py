# Utilities


import datetime

from dateutil.relativedelta import relativedelta, MO

epoch = datetime.date(1970, 1, 1)


def labelDays(df):
    df = df.assign(day=lambda x: (x["timestamp"]))


def getLastDay(date):
    if date.hour < 5:
        return date.date() + datetime.timedelta(days=-1)
    else:
        return date.date()


def getLastMonday(date):
    return date.date() + relativedelta(weekday=MO(-1))


def getDayNumber(dt):
    return (dt - epoch).days
