from pyspark.sql import SparkSession
from pyspark.sql.functions import when, initcap, col, collect_list

spark = SparkSession.builder\
    .appName("spark-program")\
    .master("local")\
    .getOrCreate()

employees = [
("karthik", 62),
("neha", 50),
("priya", 30),
("mohan", 65),
("ajay", 40),
("vijay", 47),
("veer", 55),
("aatish", 30),
("animesh", 75),
("nishad", 60)]

employees_df = spark.createDataFrame(employees, ["name", "hours_worked"])
df1 = employees_df.select(col("name"),col("hours_worked")
                          ,when(col("hours_worked")>60,"Excessive Overtime")
                          .when((col("hours_worked")>45) & (col("hours_worked")<60),"Standard Overtimr")
                          .otherwise("No Overtime")
                          .alias("Overtime Status"))
df1.show()

df2 = df1.withColumn("name",initcap(col("name")))
df2.show()

df3 = df1.groupBy("Overtime Status").agg(collect_list("name"))
df3.show()