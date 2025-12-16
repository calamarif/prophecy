from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pipeline101.config.ConfigStore import *
from pipeline101.functions import *

def load_company_reference_data(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("Firm", StringType(), True), StructField("TotalEmp", StringType(), True), StructField("SICCode", StringType(), True), StructField("MaxSegment", StringType(), True), StructField("NumEmployeeStrata", StringType(), True), StructField("Industry", StringType(), True), StructField("DUNS Number", StringType(), True), StructField("Global Ultimate DUNS", StringType(), True), StructField("Domestic Ultimate DUNS", StringType(), True), StructField("SiteEmployee#", StringType(), True), StructField("GlobalEmployee#", StringType(), True), StructField("OrigMaxSegment", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", ",")\
        .csv("dbfs:/Users/callum@prophecy.io/CompanyReferenceData.csv")
