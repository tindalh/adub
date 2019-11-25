import datetime
import pandas as pd

def clean(df, lastUpdate='2000-01-01'):
    dfResult = __arrangeClipperColumns__(df, lastUpdate)
    return dfResult 

def __getDateFromDelta__(self, delta):
    now = datetime.datetime.now()
    delta = int(delta)

    return datetime.date(now.year + ((now.month + delta) // 12), (12+(now.month + delta))%12, 1)  

def __arrangeClipperColumns__(df, firstAsOf):
    df.rename(columns={ df.columns[0]: "date_asof" }, inplace = True)
    df['date_asof'] = pd.to_datetime(df['date_asof'])
    df['date_asof'] = df['date_asof'].dt.strftime('%Y-%m-%d')
    df = df[df["date_asof"] > firstAsOf]
    return df