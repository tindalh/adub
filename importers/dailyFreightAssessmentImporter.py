# df = pd.read_excel('Daily Freight Rate Assessment_2019.xlsm')
# firstTable = df.iloc[:,0:6]

# df_RoundTrip_VLCC = None
# for i in range(len(firstTable.values)):
#     if(firstTable.values[i][0] == 'VLCC'):
#         for j in range(len(firstTable.values)):
#             if(firstTable.values[i+j][0].lower() == 'suezmax'):
#                 df_RoundTrip_VLCC = firstTable.iloc[i+1:i+j-2]
#             break

# print(df_RoundTrip_VLCC)