from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, count

spark = SparkSession.builder\
    .appName("spark-program")\
    .master("local[*]")\
    .getOrCreate()

orders = [("Order1", "Laptop", "Domestic", 2),
("Order2", "Shoes", "International", 8),
("Order3", "Smartphone", "Domestic", 3),
("Order4", "Tablet", "International", 5),
("Order5", "Watch", "Domestic", 7),
("Order6", "Headphones", "International", 10),
("Order7", "Camera", "Domestic", 1),
("Order8", "Shoes", "International", 9),
("Order9", "Laptop", "Domestic", 6),
("Order10", "Tablet", "International", 4)]

orders_df = spark.createDataFrame(orders, ["order_id", "product_type", "origin", "delivery_days"])
df1 = orders_df.withColumn("delivery speed"
                           ,when((col("delivery_days")>7) & (col("origin")=="International"),"Delayed")
                           .when(col("delivery_days").between(3,7),"On-time")
                           .when(col("delivery_days")<3,"Fast")
                           .otherwise("Unknown"))
df1.show()

df2 = df1.groupBy(col("product_type"),col("delivery speed")).agg(count("delivery speed").alias("count of each delivery speed category"))
df2.show()