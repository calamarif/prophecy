from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pipeline_using_bre.config.ConfigStore import *
from pipeline_using_bre.functions import *

def new_dataset(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("TransactionLineID", StringType(), True), StructField("AccountCode", StringType(), True), StructField("LegalEntityID", StringType(), True), StructField("TransactionDate", StringType(), True), StructField("DebitAmount", StringType(), True), StructField("CreditAmount", StringType(), True), StructField("Description", StringType(), True), StructField("CreatedBy", StringType(), True), StructField("VendorID", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", ",")\
        .csv("dbfs:/Volumes/se_demo/default/demo_volume/transactions_for_ic.csv")
