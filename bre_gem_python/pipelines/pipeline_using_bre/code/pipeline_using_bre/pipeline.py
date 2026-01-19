from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pipeline_using_bre.config.ConfigStore import *
from pipeline_using_bre.functions import *
from prophecy.utils import *
from pipeline_using_bre.graph import *

def pipeline(spark: SparkSession) -> None:
    df_new_dataset = new_dataset(spark)
    df_apply_business_rules = apply_business_rules(spark, df_new_dataset)

def main():
    spark = SparkSession.builder.enableHiveSupport().appName("pipeline_using_bre").getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/pipeline_using_bre")
    spark.conf.set("spark.default.parallelism", "4")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/pipeline_using_bre", config = Config)(pipeline)

if __name__ == "__main__":
    main()
