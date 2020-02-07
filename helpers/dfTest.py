import pandas as pd
import numpy as np

df1 = pd.DataFrame({'A': ['A0', 'A1', 'A2'],
 'B': ['B0', 'B1', 'B2']})

df2 = pd.DataFrame({'A': ['A0', 'A1','A1', 'A2'],
 'B': ['B0', 'B1', 'B2','B3']})

def condition_fn(row):
    
    return row


df = df1.merge(df2, how='outer').groupby(df2['B'])['A','B'].apply(condition_fn, axis=1)


print(df)