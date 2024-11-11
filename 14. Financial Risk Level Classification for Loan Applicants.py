from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, count, sum, avg

spark = SparkSession.builder\
    .appName("spark-program")\
    .master("local[*]")\
    .getOrCreate()

loan_applicants = [
("karthik", 60000, 120000, 590),
("neha", 90000, 180000, 610),
("priya", 50000, 75000, 680),
("mohan", 120000, 240000, 560),
("ajay", 45000, 60000, 620),
("vijay", 100000, 100000, 700),
("veer", 30000, 90000, 580),
("aatish", 85000, 85000, 710),
("animesh", 50000, 100000, 650),
("nishad", 75000, 200000, 540)]

loan_applicants_df = spark.createDataFrame(loan_applicants, ["name", "income", "loan_amount","credit_score"])

df1 = loan_applicants_df.withColumn("risk level"
                                    ,when((col("loan_amount")>col("income")) & (col("credit_score")<600),"High Risk")
                                    .when()
                                    )