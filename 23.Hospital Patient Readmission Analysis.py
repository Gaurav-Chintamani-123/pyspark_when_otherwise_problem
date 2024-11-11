from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, count, sum, avg, min, max

spark = SparkSession.builder \
    .appName("spark-program") \
    .master("local[*]") \
    .getOrCreate()

patients = [
("Patient1", 62, 10, 3, "ICU"),
("Patient2", 45, 25, 1, "General"),
("Patient3", 70, 8, 2, "ICU"),
("Patient4", 55, 18, 3, "ICU"),
("Patient5", 65, 30, 1, "General"),
("Patient6", 80, 12, 4, "ICU"),
("Patient7", 50, 40, 1, "General"),
("Patient8", 78, 15, 2, "ICU"),
("Patient9", 40, 35, 1, "General"),
("Patient10", 73, 14, 3, "ICU")]

patients_df = spark.\
    createDataFrame(patients, ["patient_id", "age", "readmission_interval","icu_admissions", "admission_type"])

df1 = patients_df.select(col("patient_id"),col("age"),col("readmission_interval"),col("icu_admissions"),col("admission_type")
                         ,when((col("readmission_interval")<15) & (col("age")>60),"high readmission risk")
                         .when(col("readmission_interval").between(15,30),"moderate risk")
                         .otherwise("low risk").alias("category"))
df1.show()

df2 = df1.groupBy("category").agg(count("patient_id"))
df2.show()

df3 = df1.filter(col("category")=="high readmission risk").agg(avg("readmission_interval"))
df3.show()

df4 = df1.filter((col("category")=="moderate risk") &
                 (col("admission_type")=="ICU") &
                 (col("icu_admissions")>2))
df4.show()