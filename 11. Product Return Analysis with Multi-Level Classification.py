from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, count

spark = SparkSession.builder\
    .appName("spark-program")\
    .master("local[*]")\
    .getOrCreate()

products = [("Laptop", "Electronics", 120, 45),
("Smartphone", "Electronics", 80, 60),
("Tablet", "Electronics", 50, 72),
("Headphones", "Accessories", 110, 47),
("Shoes", "Clothing", 90, 55),
("Jacket", "Clothing", 30, 80),
("TV", "Electronics", 150, 40),
("Watch", "Accessories", 60, 65),
("Pants", "Clothing", 25, 75),
("Camera", "Electronics", 95, 58)]

products_df = spark.createDataFrame(products, ["product_name", "category", "return_count", "satisfaction_score"])
df1 = products_df.select(col("product_name"),col("category"),col("return_count"),col("satisfaction_score")
                             ,when((col("return_count")>100) & (col("satisfaction_score")<50),"High Rate Return")
                             .when((col("return_count").between(50,100)) & (col("satisfaction_score").between(50,70)),"Moderate Return Rate")
                             .otherwise("Low Return Rate")
                         .alias("return rate"))
df1.show()

df2 = df1.groupBy(col("category"),col("return rate")).agg(count(col("return rate")))
df2.show()