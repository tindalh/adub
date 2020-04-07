import datetime
import pandas as pd

def clean(df, lastUpdate='2000-01-01'):
    dfResult = __arrangeClipperColumns__(df, lastUpdate)
    dfResult = setUploadDate(df)
    return dfResult 

def setUploadDate(df):
    
    df['UploadDate'] = datetime.date.today()
            
    return df

def __getDateFromDelta__(self, delta):
    now = datetime.datetime.now()
    delta = int(delta)

    return datetime.date(now.year + ((now.month + delta) // 12), (12+(now.month + delta))%12, 1)  

def __arrangeClipperColumns__(df, firstAsOf):
    df.rename(columns={ df.columns[0]: "date_asof" }, inplace = True)

    try:
        df['date_asof'] = pd.to_datetime(df['date_asof'], format='%d/%m/%Y')
    except:        
        try:
            df['date_asof'] = pd.to_datetime(df['date_asof'], format='%d-%m-%Y')
        except:
            try:
                df['date_asof'] = pd.to_datetime(df['date_asof'], format='%Y%m%d')
            except:
                df['date_asof'] = pd.to_datetime(df['date_asof'])
    df['date_asof'] = df['date_asof'].dt.strftime('%Y-%m-%d')
    df = df[df["date_asof"] > firstAsOf]
    return df