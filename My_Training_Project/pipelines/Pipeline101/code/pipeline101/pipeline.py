from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pipeline101.config.ConfigStore import *
from pipeline101.functions import *
from prophecy.utils import *
from pipeline101.graph import *

def pipeline(spark: SparkSession) -> None:
    df_Reformat_1 = Reformat_1(spark)
    df_OrderBy_1 = OrderBy_1(spark)
    df_load_account_bookings = load_account_bookings(spark)
    df_data_cleaning_operations = data_cleaning_operations(spark, df_load_account_bookings)
    df_load_company_reference_data = load_company_reference_data(spark)
    df_data_cleansing_operations = data_cleansing_operations(spark, df_load_company_reference_data)
    df_firm_account_join = firm_account_join(spark, df_data_cleansing_operations, df_data_cleaning_operations)
    df_Deduplicate_1 = Deduplicate_1(spark)
    df_WindowFunction_1 = WindowFunction_1(spark)

def main():
    spark = SparkSession.builder.enableHiveSupport().appName("Pipeline101").getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/Pipeline101")
    spark.conf.set("spark.default.parallelism", "4")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/Pipeline101", config = Config)(pipeline)

if __name__ == "__main__":
    main()
