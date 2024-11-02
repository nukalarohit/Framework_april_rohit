import pandas as pd
import pandasql as ps
import openpyxl
from XpandasFramework.validation_util import count_checkpanda, data_compare

spath = r"C:\Users\india\PycharmProjects\Framework_JUNE_rohit1\files\bank-salary_text.txt"
srcdf = pd.read_csv(spath)
print(srcdf)

tpath = r"C:\Users\india\PycharmProjects\Framework_JUNE_rohit1\files\bank-salary_text_target.txt"
trcdf = pd.read_csv(tpath)
print(trcdf)

#--------------data compare-----------
count_checkpanda(srcdf, trcdf)

#--------------data compare-----------
data_compare(srcdf,trcdf)

# p = r"C:\Users\india\PycharmProjects\Framework_JUNE_rohit1\config\Master_Test_Template_june.xlsx"
#
# df = pd.read_excel(p)
# print(df)
# dfsq = ps.sqldf("select * from df where execution_ind='Y'")
# print(dfsq)
# dfvalid = ps.sqldf("select validation_Type from dfsq")
# print(dfvalid)

