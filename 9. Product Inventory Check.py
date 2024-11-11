from pyspark.sql import SparkSession
from pyspark.sql.functions import when, initcap, col, count, sum

spark = SparkSession.builder\
    .appName("spark-program")\
    .master("local")\
    .getOrCreate()

inventory = [
("ProductA", 120),
("ProductB", 95),
("ProductC", 45),
("ProductD", 200),
("ProductE", 75),
("ProductF", 30),
("ProductG", 85),
("ProductH", 100),
("ProductI", 60),
("ProductJ", 20)]

inventory_df = spark.createDataFrame(inventory, ["product_name", "stock_quantity"])
df1 = inventory_df.select(col("product_name"),col("stock_quantity")
                          ,when(col("stock_quantity")>100,"overstocked")
                          .when((col("stock_quantity")>=50) & (col("stock_quantity")<=100),"normal")
                          .otherwise("low stocked")
                          .alias("stock level"))
df1.show()

df2 = df1.groupBy(col("stock level")).agg(sum("stock_quantity").alias("count"))
df2.show()



