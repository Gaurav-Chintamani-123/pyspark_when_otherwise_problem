from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, count, sum, avg, min, max

spark = SparkSession.builder \
    .appName("spark-program") \
    .master("local[*]") \
    .getOrCreate()

ecommerce_return = [
("Product1", 75, 25),
("Product2", 40, 15),
("Product3", 30, 5),
("Product4", 60, 18),
("Product5", 100, 30),
("Product6", 45, 10),
("Product7", 80, 22),
("Product8", 35, 8),
("Product9", 25, 3),
("Product10", 90, 12)]

ecommerce_return_df = spark.createDataFrame(ecommerce_return, ["product_name", "sale_price","return_rate"])

df1 = ecommerce_return_df.select(col("product_name"),col("sale_price"),col("return_rate")
                                 ,when(col("return_rate") > 20, "high return")
                                 .when(col("return_rate").between(10, 20), "medium return")
                                 .otherwise("low return")
                                 .alias("category"))
df1.show()


df2 = df1.groupBy("category").agg(count("product_name").alias("Count products"))
df2.show()

df3 = df1.filter(col("category") == "high return").agg(avg("sale_price"))
df3.show()

df4 = df1.filter(col("category") == "medium return").agg(max("sale_price"))
df4.show()

df5 = df1.filter((col("category") == "low return") &
                 (col("sale_price") < 50) &
                 (col("return_rate") < 5))
df5.show()
