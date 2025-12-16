from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pipeline101.config.ConfigStore import *
from pipeline101.functions import *

def firm_account_join(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0.alias("in0").join(in1.alias("in1"), (col("in0.Firm") == col("in1.Account")), "inner")
