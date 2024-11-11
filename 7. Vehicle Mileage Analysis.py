from pyspark.sql import SparkSession
from pyspark.sql.functions import when, initcap, col, count

spark = SparkSession.builder\
    .appName("spark-program")\
    .master("local")\
    .getOrCreate()

vehicles = [
("CarA", 30),
("CarB", 22),
("CarC", 18),
("CarD", 15),
("CarE", 10),
("CarF", 28),
("CarG", 12),
("CarH", 35),
("CarI", 25),
("CarJ", 16)]

vehicles_df = spark.createDataFrame(vehicles, ["vehicle_name", "mileage"])
df1 = vehicles_df.select(col("vehicle_name"),col("mileage")
                         ,when(col("mileage")>25,"High Efficiency")
                         .when((col("mileage")>=15) & (col("mileage")<=25),"Moderate Efficiency")
                         .otherwise("Low Efficiency").alias("category")
                         )
df1.show()
