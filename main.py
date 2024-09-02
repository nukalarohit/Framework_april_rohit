import pandas as pd
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_set
from utility.read_utility import read_file, read_db
from utility.validation_lib import count_check, duplicate, uniqueness_check, records_present_only_in_source,records_present_only_target

project_path = os.getcwd()
jar_path = project_path + "/jars/ojdbc11-21.1.0.0.jar"
spark = SparkSession.builder.master("local[1]") \
    .appName("test") \
    .config("spark.jars", jar_path) \
    .config("spark.driver.extraClassPath", jar_path) \
    .config("spark.executor.extraClassPath", jar_path) \
    .getOrCreate()

out = {'validation_type': [],
       'source_name': [],
       'target_name': [],
       'no_of_source_record_count': [],
       'no_of_target_record_count': [],
       'failed_count': [],
       'column': [],
       'status': [],
       'source_type': [],
       'target_type': []}

tc = pd.read_excel(r"C:\Users\india\PycharmProjects\Framework_april_rohit1\config\Master_Test_Template.xlsx")
# print(tc)
run_test_case = tc.loc[(tc.execution_ind == 'Y')]
# print(run_test_cases)
run_test_case = spark.createDataFrame(run_test_case)
# df.show()
validation_df = (run_test_case.groupBy('source', 'source_type',
                                       'source_db_name', 'source_schema_path', 'source_transformation_query_path',
                                       'target', 'target_type', 'target_db_name', 'target_schema_path',
                                       'target_transformation_query_path',
                                       'key_col_list', 'null_col_list', 'exclude_columns',
                                       'unique_col_list', 'dq_column', 'expected_values', 'min_val', 'max_val').
                 agg(collect_set('validation_Type').alias('validation_Type')))
# validation.show(truncate=False)
print('validation:  ', validation_df)
validations = validation_df.collect()
print(validations)

for row in validations:
    if row['source_type'] == 'table':
        source = read_db(spark=spark, database=row['source_db_name'],
                         transformation_query_path=row['source_transformation_query_path'])
    else:
        source = read_file(format=row['source_type'], path=row['source'], spark=spark)

    if row['target_type'] == 'table':
        target = read_db(spark=spark, database=row['target_db_name'],
                         transformation_query_path=row['target_transformation_query_path'])
    else:
        target = read_file(format=row['target_type'], path=row['target'], spark=spark)
    source.show()
    target.show()
    print("***********************  Validations Started  *********************************")
    for validation in row['validation_Type']:
        if validation == 'count_check':
            count_check(source, target, out, row, validation)
        elif validation == 'duplicate':
            duplicate(target, row['key_col_list'], out, row, validation)
        elif validation == 'uniqueness_check':
            uniqueness_check(target, row['unique_col_list'], out, row, validation)
        elif validation == 'records_present_only_in_source':
            records_present_only_in_source(source, target, row['key_col_list'], row, out, validation)
        elif validation=='records_present_only_target':
            records_present_only_target(source, target, row['key_col_list'], row, out, validation)

summary = pd.DataFrame(out)
print(summary)
summary.to_csv(r"C:\Users\india\PycharmProjects\Framework_april_rohit1\execution_summary\summary.csv")
