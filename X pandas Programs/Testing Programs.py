import pandas as pd
import pandasql as ps

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 2000)
df = pd.read_csv("C:/Users/india/PycharmProjects/Framework_JUNE_rohit1/files/Contact_info_source.csv")
print(df)
print(df.loc[3, 'Surname'])

print(df.loc[0, 'Identifier'])
print(df.loc[0, ['Identifier', 'Surname']])
print(df.loc[[0, 1, 2, 3, 4], ['Surname', 'suffix']])

print(df.columns)
print(df[['Surname','Identifier']])

print(df.loc[[1,2,3,4]])

print(df[['Surname','Identifier']])
print(df.loc[[3],['Surname','Identifier']])