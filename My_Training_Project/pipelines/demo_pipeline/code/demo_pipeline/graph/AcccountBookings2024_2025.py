from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from demo_pipeline.config.ConfigStore import *
from demo_pipeline.functions import *

def AcccountBookings2024_2025(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("Account", StringType(), True), StructField("Address", StringType(), True), StructField("Address Line 2", StringType(), True), StructField("City", StringType(), True), StructField("State", StringType(), True), StructField("ZipCode", StringType(), True), StructField("Country", StringType(), True), StructField("MainPhoneNumber", StringType(), True), StructField("Bookings in 2024", StringType(), True), StructField("Bookings in 2025", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", ",")\
        .csv("dbfs:/Users/callum@prophecy.io/AccountBookings2024_25.csv")
