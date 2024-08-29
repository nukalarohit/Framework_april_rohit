from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()


def read_file(format, path, spark, schema='NOT APPL'):
    format = format.lower()
    if format == 'csv':
        if schema != 'NOT APPL':
            df = spark.read.format("csv").option("schema", schema).load(path)
        else:
            df = (spark.read.format("csv").option("inferSchema", True).
                  option("header", True).option("seperator", ',').load(path))
            return df
    # elif format == 'jason':
    #     spark.read.json("path")
    # elif format == 'parquet':
    #     spark.read.parquet("path")
    # elif format == 'avro':
    #     spark.read.format("avro").load("path")
    # return df