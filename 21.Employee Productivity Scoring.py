from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, count, sum, avg, min, max

spark = SparkSession.builder \
    .appName("spark-program") \
    .master("local[*]") \
    .getOrCreate()

employee_productivity = [
("Emp1", 85, 6),
("Emp2", 75, 4),
("Emp3", 40, 1),
("Emp4", 78, 5),
("Emp5", 90, 7),
("Emp6", 55, 3),
("Emp7", 80, 5),
("Emp8", 42, 2),
("Emp9", 30, 1),
("Emp10", 68, 4)]

employee_productivity_df = spark\
    .createDataFrame(employee_productivity, ["employee_id","productivity_score", "project_count"])

df1 = employee_productivity_df.select(col("employee_id"),col("productivity_score"),col("project_count")
                                      ,when((col("productivity_score")>80) & (col("project_count")>5),"high performer")
                                      .when(col("productivity_score").between(60,80),"average performer")
                                      .otherwise("low performer")
                                      .alias("category"))
df1.show()

df2 = df1.groupBy("category").agg(count("employee_id").alias("count"))
df2.show()

df3 = df1.filter(col("category")=="high performer").agg(avg("productivity_score"))
df3.show()

df4 = df1.filter(col("category")=="average performer").agg(min("productivity_score"))
df4.show()

df5 = df1.filter((col("category")=="low performer") &
                 (col("productivity_score")<50) &
                 (col("project_count")<2))
df5.show()