from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, sum, collect_list, struct

spark = SparkSession.builder\
    .appName("spark-program")\
    .master("local[*]")\
    .getOrCreate()

employees = [("karthik", "Sales", 85),
("neha", "Marketing", 78),
("priya", "IT", 90),
("mohan", "Finance", 65),
("ajay", "Sales", 55),
("vijay", "Marketing", 82),
("veer", "HR", 72),
("aatish", "Sales", 88),
("animesh", "Finance", 95),
("nishad", "IT", 60)]

employees_df = spark.createDataFrame(employees, ["name", "department", "performance_score"])
df1 = employees_df.select(col("name"),col("department"),col("performance_score")
                          ,when((col("department").isin("Sales","Marketing")) & (col("performance_score")>80),0.20)
                          .when(col("performance_score")>80,0.15)
                          .otherwise(0.0)
                          .alias("bonus percentage")
                          )
df1.show()

df1 = df1.withColumn("bouns amount",col("bonus percentage")*100)
df1.show()

df2 = df1.select(col("name"),col("bonus percentage"))\
    .groupBy("department").agg(sum(col("bonus amount")).alias("total_bonus_amount"))
df2.show()