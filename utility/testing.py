from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()
df=spark.read.format("parquet").load(r"C:\Users\india\PycharmProjects\Bobby\FILES\userdata1.parquet")
df.sample(.01,seed=1).show()