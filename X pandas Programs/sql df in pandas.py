import pandas as pd
import pandasql as ps
pd.set_option('display.max_columns',None)
pd.set_option('display.width',2000)
df = pd.read_csv(r"C:\Users\india\PycharmProjects\Framework_JUNE_rohit1\files\Contact_info_source.csv")

sdf = ps.sqldf("select Identifier,count(*) from df group by  Identifier having count(*)>1")
print(type(sdf))
sdfcnt=len(sdf)
print(sdf)
print(type(sdfcnt))
print(sdfcnt)

if sdfcnt>0:
    print("duplicates exists")
else:
    print("NO duplicates ")