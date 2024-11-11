from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, avg

spark = SparkSession.builder\
    .appName("spark-program")\
    .master("local[*]")\
    .getOrCreate()

customers = [("karthik", "Premium", 1050, 32),
("neha", "Standard", 800, 28),
("priya", "Premium", 1200, 40),
("mohan", "Basic", 300, 35),
("ajay", "Standard", 700, 25),
("vijay", "Premium", 500, 45),
("veer", "Basic", 450, 33),
("aatish", "Standard", 600, 29),
("animesh", "Premium", 1500, 60),
("nishad", "Basic", 200, 21)]

customers_df = spark.createDataFrame(customers, ["name", "membership", "spending", "age"])
df1 = customers_df.select(col("name"),col("membership"),col("spending"),col("age")
                              ,when((col("spending")>1000) & (col("membership") == "Premium"),"High Spender")
                              .when((col("spending").between(500,1000)) & (col("membership")=="Standard"),"Average Spender")
                              .otherwise("Low Spender").alias("spending category"))
df1.show()

df2 = df1.groupBy(col("membership"),col("name"),col("spending category"))\
    .agg(avg(col("spending")).alias("avg spending"))
df2.show()