from pyspark.sql import SparkSession
from pyspark.sql.functions import when,col

spark = SparkSession.builder\
    .appName("spark-program")\
    .master("local")\
    .getOrCreate()

sales = [
("karthik", 60000),
("neha", 48000),
("priya", 30000),
("mohan", 24000),
("ajay", 52000),
("vijay", 45000),
("veer", 70000),
("aatish", 23000),
("animesh", 15000),
("nishad", 8000),
("varun", 29000),
("aadil", 32000)
]

sales_df = spark.createDataFrame(sales, ["name", "total_sales"])

df1 = sales_df.select(col("name"),col("total_sales")
                      ,when(col("total_sales")>50000,"Excellent")
                      .when((col("total_sales")>=25000) & (col("total_sales")<=50000),"Good")
                      .otherwise("Needs Improvement")
                      .alias("performance_status"))
df1.show()