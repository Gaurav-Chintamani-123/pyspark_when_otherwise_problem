from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, count, sum, avg, min, max

spark = SparkSession.builder\
    .appName("spark-program")\
    .master("local[*]")\
    .getOrCreate()

electricity_usage = [
("House1", 550, 250),
("House2", 400, 180),
("House3", 150, 50),
("House4", 500, 200),
("House5", 600, 220),
("House6", 350, 120),
("House7", 100, 30),
("House8", 480, 190),
("House9", 220, 105),
("House10", 150, 60)]

electricity_usage_df = spark.createDataFrame(electricity_usage, ["household", "kwh_usage", "total_bill"])

df1 = electricity_usage_df.select(col("household"),col("kwh_usage"),col("total_bill")
                                  ,when((col("kwh_usage") > 500) & (col("total_bill") > 200), "high usage")
                                  .when(col("kwh_usage").between(200, 500) & col("total_bill").between(100, 200),"medium usage")
                                  .otherwise("low usage")
                                  .alias("usage level")
                                  )
df1.show()

df2 = df1.groupBy("usage level").agg(count("household").alias("total number of households "))
df2.show()

df3 = df1.filter(col("usage level")=="high usage").agg(max("total_bill").alias("maximum bill amount"))
df3.show()

df4 = df1.filter(col("usage level")=="medium usage").agg(avg("kwh_usage").alias("average_kwh"))
df4.show()

df5 = df1.filter((col("usage level")=="low usage") & (col("kwh_usage")>300)).agg(count("household").alias("count_household"))
df5.show()