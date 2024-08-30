import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_set
from utility.read_utility import read_file

spark = SparkSession.builder.getOrCreate()

tc = pd.read_excel(r"C:\Users\india\PycharmProjects\Framework_april_rohit1\config\Master_Test_Template.xlsx")
# print(tc)
run_test_case = tc.loc[(tc.execution_ind == 'Y')]
# print(run_test_cases)
run_test_case = spark.createDataFrame(run_test_case)
# df.show()
validation = (run_test_case.groupBy('source', 'source_type',
                                    'source_db_name', 'source_schema_path', 'source_transformation_query_path',
                                    'target', 'target_type', 'target_db_name', 'target_schema_path',
                                    'target_transformation_query_path',
                                    'key_col_list', 'null_col_list', 'exclude_columns',
                                    'unique_col_list', 'dq_column', 'expected_values', 'min_val', 'max_val').
              agg(collect_set('validation_Type').alias('validation_Type')))
# validation.show(truncate=False)
validations = validation.collect()
print(validations)
for row in validations:
    source = read_file(format=row['source_type'], path=row['source'], spark=spark)
    source.show()
