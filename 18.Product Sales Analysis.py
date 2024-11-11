from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, count, sum, avg, min, max

spark = SparkSession.builder \
    .appName("spark-program") \
    .master("local[*]") \
    .getOrCreate()

product_sales = [
("Product1", 250000, 5),
("Product2", 150000, 8),
("Product3", 50000, 20),
("Product4", 120000, 10),
("Product5", 300000, 7),
("Product6", 60000, 18),
("Product7", 180000, 9),
("Product8", 45000, 25),
("Product9", 70000, 15),
("Product10", 10000, 30)]

product_sales_df = spark.createDataFrame(product_sales, ["product_name", "total_sales", "discount"])
df1 = product_sales_df.select(col("product_name"),col("total_sales"),col("discount")
                              ,when((col("total_sales")>200000) & (col("discount")<10),"top seller")
                              .when(col("total_sales").between(100000,200000),"moderate seller")
                              .otherwise("low seller").alias("category"))
df1.show()

df2 = df1.groupBy("category").agg(count("product_name").alias("count"))
df2.show()

df3 = df1.filter(col("category") =="top seller").agg(max("total_sales").alias("max_sales"))
df3.show()

df4 = df1.filter(col("category") == "moderate seller").agg(min("discount").alias("min_discount"))
df4.show()


df5 = df1.filter((col("category") == "low seller") &
                 (col("total_sales") < 50000) &
                 (col("discount") > 15))
df5.show()