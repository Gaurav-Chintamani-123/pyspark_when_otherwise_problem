from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, count, sum, avg, min, max

spark = SparkSession.builder \
    .appName("spark-program") \
    .master("local[*]") \
    .getOrCreate()

students = [
("Student1", 70, 45, 60, 65, 75),
("Student2", 80, 55, 58, 62, 67),
("Student3", 65, 30, 45, 70, 55),
("Student4", 90, 85, 80, 78, 76),
("Student5", 72, 40, 50, 48, 52),
("Student6", 88, 60, 72, 70, 68),
("Student7", 74, 48, 62, 66, 70),
("Student8", 82, 56, 64, 60, 66),
("Student9", 78, 50, 48, 58, 55),
("Student10", 68, 35, 42, 52, 45)]

students_df = spark\
    .createDataFrame(students, ["student_id", "attendance_percentage","math_score", "science_score", "english_score", "history_score"])

average_score = students_df.withColumn("avg_score",
          (col("math_score")
            + col("science_score")
            + col("english_score")
            + col("history_score")) / 4)
average_score.show()

df1 = average_score.select(col("student_id"),
                         col("attendance_percentage"),
                         col("math_score"),
                         col("science_score"),
                         col("english_score"),
                         col("history_score"),
                         when((col("attendance_percentage") < 70) & (col("avg_score") < 50), "at-risk")
                         .when((col("attendance_percentage").between(75, 85)), "moderate risk")
                         .otherwise("low risk").alias("category"))
df1.show()

df2 = df1.groupBy("category").agg(count("student_id"))
df2.show()

df3 = df1.filter(col("category") == "At-Risk").agg(avg("average_score").alias("avg_at_risk_score"))
df3.show()

df4 = df1.filter(col("category") == "moderate risk").filter(array(col("math_score")
                                                                  ,col("science_score")
                                                                  ,col("english_score")
                                                                  ,col("history_score")))
df4.show()
