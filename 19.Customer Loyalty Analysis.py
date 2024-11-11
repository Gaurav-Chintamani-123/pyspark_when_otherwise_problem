from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, count, sum, avg, min, max

spark = SparkSession.builder \
    .appName("spark-program") \
    .master("local[*]") \
    .getOrCreate()

customer_loyalty = [
("Customer1", 25, 700),
("Customer2", 15, 400),
("Customer3", 5, 50),
("Customer4", 18, 450),
("Customer5", 22, 600),
("Customer6", 2, 80),
("Customer7", 12, 300),
("Customer8", 6, 150),
("Customer9", 10, 200),
("Customer10", 1, 90)]

customer_loyalty_df = spark.createDataFrame(customer_loyalty, ["customer_name","purchase_frequency", "average_spending"])
df1 = customer_loyalty_df.select(col("customer_name"),col("purchase_frequency"),col("average_spending")
                                  ,when((col("purchase_frequency")>20) & (col("average_spending")>500),"highly loyal")
                                  .when(col("purchase_frequency").between(10,20),"moderately loyal")
                                  .otherwise("low loyal").alias("category")
                                  )
df1.show()

df2 = df1.groupBy("category").agg(count("customer_name").alias("count"))
df2.show()

df3 = df1.filter(col("category") == "highly loyal").agg(avg("average_spending").alias("avg spending highly loyal"))
df3.show()

df4 = df1.filter(col("category") == "moderately loyal").agg(min("average_spending").alias("min spending mod loyal"))
df4.show()

df5 = df1.filter((col("category") == "low loyal") &
                 (col("average_spending") < 100) &
                 (col("purchase_frequency") < 5))
df5.show()
