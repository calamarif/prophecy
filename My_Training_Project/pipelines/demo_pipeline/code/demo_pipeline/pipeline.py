from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from demo_pipeline.config.ConfigStore import *
from demo_pipeline.functions import *
from prophecy.utils import *
from demo_pipeline.graph import *

def pipeline(spark: SparkSession) -> None:
    df_AcccountBookings2024_2025 = AcccountBookings2024_2025(spark)

def main():
    spark = SparkSession.builder.enableHiveSupport().appName("demo_pipeline").getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/demo_pipeline")
    spark.conf.set("spark.default.parallelism", "4")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/demo_pipeline", config = Config)(pipeline)

if __name__ == "__main__":
    main()
