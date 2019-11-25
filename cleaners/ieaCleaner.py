import pandas

def __replaceInvalidQuantities__(df):
    dfReplaced = df
    if('Quantity' in df.columns):        
        df['Quantity'].replace(["x"], '0',inplace=True)

    return dfReplaced

def clean(df):
    dfResult = __replaceInvalidQuantities__(df)
    return dfResult