from pyspark.sql import SparkSession
from pyspark.sql.functions import when, initcap, col, count

spark = SparkSession.builder\
    .appName("spark-program")\
    .master("local")\
    .getOrCreate()

customers = [
("karthik", 22),
("neha", 28),
("priya", 40),
("mohan", 55),
("ajay", 32),
("vijay", 18),
("veer", 47),
("aatish", 38),
("animesh", 60),
("nishad", 25)]

customers_df = spark.createDataFrame(customers, ["name", "age"])
df1 = customers_df.select(col("name"),col("age"),
                          when(col("age")<25,"Youth")
                          .when((col("age")>=25) & (col("age")<=45),"Adult")
                          .otherwise("Senior")
                          .alias("category")
                          )
df1.show()

df2 = df1.groupBy(col("category")).agg(count("name").alias("count"))
df2.show()

df3 = df1.select(initcap(col("name")).alias("name"))
df3.show()
