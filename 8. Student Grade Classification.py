from pyspark.sql import SparkSession
from pyspark.sql.functions import when, initcap, col, count

spark = SparkSession.builder\
    .appName("spark-program")\
    .master("local")\
    .getOrCreate()

students = [
("karthik", 95),
("neha", 82),
("priya", 74),
("mohan", 91),
("ajay", 67),
("vijay", 80),
("veer", 85),
("aatish", 72),
("animesh", 90),
("nishad", 60)]

students_df = spark.createDataFrame(students, ["name", "score"])
df1 = students_df.select(col("name"),col("score")
                                ,when(col("score")>=90,"Excellent")
                                .when((col("score")>=75) & (col("score")<=89),"Good")
                                .otherwise("Needs Improvement")
                                .alias("category"))
df1.show()

df2 = df1.groupBy(col("category")).agg(count("category").alias("count"))
df2.show()