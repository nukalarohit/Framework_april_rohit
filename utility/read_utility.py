import json
import os
from pyspark.sql import SparkSession
from utility.general_utility import flatten

# spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()


def read_file(format, path, spark, schema='NOT APPL', multiline=True):
    format = format.lower()
    if format == 'csv':
        if schema != 'NOT APPL':
            df = spark.read.format("csv").option("schema", schema).load(path)
        else:
            df = (spark.read.format("csv").option("inferSchema", True).
                  option("header", True).option("seperator", ',').load(path))
    elif format == 'json':
        if multiline == True:
            df = spark.read.format("json").option("multiline", True).load(path)
            df = flatten(df)
        else:
            df = spark.read.format("json").option("multiline", False).load(path)
            df = flatten(df)
    elif format == 'parquet':
        df = spark.read.format("parquet").load(path)
    # elif format == 'avro':
    #     df = spark.read.format('avro').load(path)
    else:
        raise ValueError("unsupported file format", format)
    return df


def read_db(spark, database, transformation_query_path):
    with open(r"C:\Users\india\PycharmProjects\Framework_april_rohit1\config\config.json") as f:
        config_data = json.load(f)[database]
    with open(transformation_query_path, "r") as file:
        sql_query = file.read()

    df = spark.read.format("jdbc"). \
        option("url", config_data['url']). \
        option("user", config_data['user']). \
        option("password", config_data['password']). \
        option("query", sql_query). \
        option("driver", config_data['driver']).load()
    return df
#
# def read_snowflake(spark,databse,transformation_query_path):
#     with open(r"C:\Users\india\PycharmProjects\Framework_april_rohit1\config\config.json") as f:
#         config_data = json.load(f)[databse]
#     with open(transformation_query_path, "r") as file:
#         sql_query = file.read()
#         df = spark.read.format("jdbc"). \
#             option("driver", "net.snowflake.client.jdbc.SnowflakeDriver"). \
#             option("url", config_data['url']). \
#             option("user", config_data['user']). \
#             option("password", config_data['password']). \
#             option("query", sql_query). \
#             option("driver", config_data['driver']).load()
#         return df


