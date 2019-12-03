import pandas as pd
import datetime
import calendar

def clean(df):
    dfResult = __getWeightedSulphur__(df)
    dfResult = __arrangeColumns__(dfResult)
    return dfResult

def __createDate__(row):
        return datetime.datetime(year=int(row['Year']), month=list(calendar.month_abbr).index(row['Month'][:3]), day=1)

def __setWeightedSulphur__(row):
    """
        pandas.core.series.Series -> float
        Consumes a row and returns the volume weighted sulphur
    """
    if(row['Crude Stream'].lower() == 'not specified'):
        if(row['Sulphur Detail'] is not 'Unknown'):
            return row['Sulphur Detail']
        else:
            print('not float')
    else:
        if(row['Sum_y'] == 0):
            round(row['ProductionSulphur_y'] ,2)
        else:
            return round(row['ProductionSulphur_y'] / row['Sum_y'] ,2)
    

def __getWeightedSulphur__(df):
    df['Sulphur Detail'].replace('Unknown', '0', inplace=True)
    df['ProductionSulphur'] = df['Sum'] * df['Sulphur Detail'].replace(["'Unknown'"], '0').astype(float)

    dfGroupName = df.groupby(['Year','Month','Crude Stream']).agg({'ProductionSulphur': 'sum',
                                                            'Sum':'sum'
                                                            })

    df = pd.merge(df, dfGroupName, on=['Year','Month','Crude Stream'])
    df['WeightedSulphur'] = df.apply(__setWeightedSulphur__, axis=1)

    return df

def __arrangeColumns__(df):
    
    df['Period'] = df.apply(__createDate__, axis=1)
    df = df.drop(['Year', 'Month', '[Data Values]'], axis=1)
    new_col=['' for x in df.index]
    df.insert(0, 'Id', new_col)
    cols = df.columns
    cols = [cols[2], cols[3],
         cols[1], cols[11], cols[6], cols[4], cols[5], cols[10]]
    return df[cols]