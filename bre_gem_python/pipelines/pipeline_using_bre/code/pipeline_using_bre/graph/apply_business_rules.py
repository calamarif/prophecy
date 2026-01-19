from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pipeline_using_bre.config.ConfigStore import *
from pipeline_using_bre.functions import *

def apply_business_rules(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return add_rule(in0, bre_gem_function())
