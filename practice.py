import json
import os
from pyspark.sql import SparkSession
from utility.general_utility import flatten, read_config, read_schema, fetch_transformation_query_path, fetch_file_path

spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()

# with open(r"C:\Users\india\PycharmProjects\Framework_april_rohit1\config\config.json") as f:
#     config_data = json.load(f)
#     print(config_data['url'])

project_path=os.getcwd()
print(project_path)