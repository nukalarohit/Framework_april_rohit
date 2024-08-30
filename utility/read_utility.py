from pyspark.sql import SparkSession
from utility.general_utility import flatten, read_config, read_schema, fetch_transformation_query_path, fetch_file_path

spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()


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
    return df
