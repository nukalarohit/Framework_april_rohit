import pandas as pd
import pandasql as ps

p = r"C:\Users\india\PycharmProjects\Framework_JUNE_rohit1\files\bank-salary_text.txt"
df = pd.read_csv(p)

# ---------------------DUPLICATES-----------------
# print(df[df.duplicated()])
#
# print(df[df['empid'].duplicated()])
#
# # ---------------------LASRGEST-----------------
# # print(df['salary'].nlargest(2))
# f = df[df['salary'] > 10000]
# print(f)
#
# dfsq=ps.sqldf("select * from df")
# print(dfsq)
# print(dfsq.count())
# print(len(dfsq))
dfsq=ps.sqldf("select * from df where salary is null")
print(dfsq)
dfsqcnt=len(dfsq)
print(dfsqcnt)

if dfsqcnt>0:
    print("NULL exists in salary")
else:
    print("NO NULLS")