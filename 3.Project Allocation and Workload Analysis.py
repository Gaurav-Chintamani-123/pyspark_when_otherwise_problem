from pyspark.sql import SparkSession
from pyspark.sql.functions import when,col

spark = SparkSession.builder\
    .appName("spark-program")\
    .master("local")\
    .getOrCreate()

workload = [
("karthik", "ProjectA", 120),
("karthik", "ProjectB", 100),
("neha", "ProjectC", 80),
("neha", "ProjectD", 30),
("priya", "ProjectE", 110),
("mohan", "ProjectF", 40),
("ajay", "ProjectG", 70),
("vijay", "ProjectH", 150),
("veer", "ProjectI", 190),
("aatish", "ProjectJ", 60),
("animesh", "ProjectK", 95),
("nishad", "ProjectL", 210),
("varun", "ProjectM", 50),
("aadil", "ProjectN", 90)]

workload_df = spark.createDataFrame(workload, ["name", "project", "hours"])

df1 = workload_df.select(col("name"),col("project"),col("hours")
                         ,when(col("hours")>200,"Overloaded")
                         ,when((col("hours")>100) & (col("hours")<200),"Balanced")
                         .otherwise("Underutilized")
                         .alias("workload level")
                         )
df1.show()