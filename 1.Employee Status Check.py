from pyspark.sql import SparkSession
from pyspark.sql.functions import when,initcap,current_date,col,datediff

spark = SparkSession.builder\
    .appName("spark-program")\
    .master("local")\
    .getOrCreate()

employees = [("karthik", "2024-11-01"),
("neha", "2024-10-20"),
("priya", "2024-10-28"),
("mohan", "2024-11-02"),
("ajay", "2024-09-15"),
("vijay", "2024-10-30"),
("veer", "2024-10-25"),
("aatish", "2024-10-10"),
("animesh", "2024-10-15"),
("nishad", "2024-11-01"),
("varun", "2024-10-05"),
("aadil", "2024-09-30")]

employees_df = spark.createDataFrame(employees, ["name", "last_checkin"])
df1 = current_date()
df2 = employees_df.withColumn("category",
                              when(datediff(df1,col("last_checkin"))<=7,"Active")
                              .otherwise("Inactive"))
df3 = df2.withColumn("name",initcap(col("name")))
df3.show()