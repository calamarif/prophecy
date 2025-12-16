from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pipeline101.config.ConfigStore import *
from pipeline101.functions import *

def Reformat_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0
