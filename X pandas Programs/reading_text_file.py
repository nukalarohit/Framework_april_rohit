import pandas as pd
import pandasql as ps
# #
# # # #------READING CSV/Comma seperated TEXT File-------------
# p=r"C:\Users\india\PycharmProjects\Framework_JUNE_rohit1\files\bank-salary_text.txt"
# df=pd.read_csv(p)
# print(df)
# #----------SQL-----------------
# ds=ps.sqldf("select A.*,salary*100 as new_salary from df A where empid=123")
# print(ds)
# #
# # #------READING CSV File-------------
# # pcsv=r"C:\Users\india\PycharmProjects\Framework_JUNE_rohit1\files\Contact_info_source.csv"
# # dfcsv=pd.read_csv(pcsv)
# # print("------READING CSV File-------------")
# # print(dfcsv)
# #
# # #------READING EXCEL File-------------
# # pexcel=r"C:\Users\india\PycharmProjects\Framework_JUNE_rohit1\files\employee.xlsx"
# # dfexcel=pd.read_excel(pexcel)
# # print("------READING EXCEL File-------------")
# # print(dfexcel)
# #
# # #------READING semicolon TEXT  File-------------
# # semitext=r"C:\Users\india\PycharmProjects\Framework_JUNE_rohit1\files\bank-salary_semicolo_text.txt"
# # dfsemitext=pd.read_csv(semitext,sep=';')
# # print("------READING semicolon TEXT File-------------")
# # print(dfsemitext)
# #
# # #------READING JSON  File-------------
# # jp = r"C:\Users\india\PycharmProjects\Framework_JUNE_rohit1\files\yelp_training_set_business.json"
# #
# # # # Read the JSON file into a DataFrame
# # # dfjson = pd.read_json(jp)
# # #
# # # # Print the DataFrame
# # # print(dfjson)
# #
# #------READING PARQUET  File-------------
# pp = r"C:\Users\india\PycharmProjects\Framework_JUNE_rohit1\files\userdata1.parquet"
# dfparq=pd.read_parquet(pp)
# print("------READING PARQUET File-------------")
# print(dfparq)
# #------WRITE PARQUET  File DF to CSV-------------
# dfparq.to_csv( r"C:\Users\india\PycharmProjects\Framework_JUNE_rohit1\files\parquettocsvfile.csv")
# #------WRITE PARQUET  File DF to EXCEL-------------
# dfparq.to_excel( r"C:\Users\india\PycharmProjects\Framework_JUNE_rohit1\files\parquettoexcelfile.xlsx")
# #------WRITE PARQUET  File DF to JSON-------------
# dfparq.to_json( r"C:\Users\india\PycharmProjects\Framework_JUNE_rohit1\files\parquettojsonfile.json")

#how to read a  CSV file that has NO HEADERS
# pnhead=r"C:\Users\india\PycharmProjects\Framework_JUNE_rohit1\files\Contact_info_source_no_headers.csv"
# cols=['rohitIdentifier','Surname','givenname','middleinitial','suffix','Primary_street_number','primary_street_name','city','state','zipcode','Primary_street_number_prev','primary_street_name_prev','city_prev','state_prev','zipcode_prev','Email','Phone','birthmonth']
# dfnoheaders=pd.read_csv(pnhead,header=None,names=cols)
# print(dfnoheaders)
