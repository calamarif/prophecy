from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pipeline101.config.ConfigStore import *
from pipeline101.functions import *

def data_cleansing_operations(spark: SparkSession, df: DataFrame) -> DataFrame:
    from pyspark.sql.functions import col, trim, regexp_replace, lower, upper, initcap
    from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType, LongType, ShortType
    # Remove rows where all columns are null
    df = df.na.drop(how = "all")
    # Step 2: Apply data cleansing operations
    # Start with the original columns
    transformed_columns = []

    # Check if column exists after null operations
    if "Firm" not in df.columns:
        print("Warning: Column 'Firm' not found after null operation. Skipping transformations for this column.")
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(df.schema["Firm"].dataType, StringType):
            transformed_columns = [initcap(trim(col("Firm"))).alias("Firm")]
        elif isinstance(df.schema["Firm"].dataType, (IntegerType, FloatType, DoubleType, LongType, ShortType)):
            transformed_columns = [col("Firm")]
        else:
            transformed_columns = [col("Firm")]

    # Check if column exists after null operations
    if "TotalEmp" not in df.columns:
        print("Warning: Column 'TotalEmp' not found after null operation. Skipping transformations for this column.")
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(df.schema["TotalEmp"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(initcap(trim(col("TotalEmp"))).alias("TotalEmp"))
        elif isinstance(df.schema["TotalEmp"].dataType, (IntegerType, FloatType, DoubleType, LongType, ShortType)):
            transformed_columns.append(col("TotalEmp"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("TotalEmp"))

    # Check if column exists after null operations
    if "SICCode" not in df.columns:
        print("Warning: Column 'SICCode' not found after null operation. Skipping transformations for this column.")
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(df.schema["SICCode"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(initcap(trim(col("SICCode"))).alias("SICCode"))
        elif isinstance(df.schema["SICCode"].dataType, (IntegerType, FloatType, DoubleType, LongType, ShortType)):
            transformed_columns.append(col("SICCode"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("SICCode"))

    # Check if column exists after null operations
    if "MaxSegment" not in df.columns:
        print(
            "Warning: Column 'MaxSegment' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(df.schema["MaxSegment"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(initcap(trim(col("MaxSegment"))).alias("MaxSegment"))
        elif isinstance(df.schema["MaxSegment"].dataType, (IntegerType, FloatType, DoubleType, LongType, ShortType)):
            transformed_columns.append(col("MaxSegment"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("MaxSegment"))

    # Check if column exists after null operations
    if "NumEmployeeStrata" not in df.columns:
        print(
            "Warning: Column 'NumEmployeeStrata' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(df.schema["NumEmployeeStrata"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(initcap(trim(col("NumEmployeeStrata"))).alias("NumEmployeeStrata"))
        elif isinstance(
            df.schema["NumEmployeeStrata"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("NumEmployeeStrata"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("NumEmployeeStrata"))

    # Check if column exists after null operations
    if "Industry" not in df.columns:
        print("Warning: Column 'Industry' not found after null operation. Skipping transformations for this column.")
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(df.schema["Industry"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(initcap(trim(col("Industry"))).alias("Industry"))
        elif isinstance(df.schema["Industry"].dataType, (IntegerType, FloatType, DoubleType, LongType, ShortType)):
            transformed_columns.append(col("Industry"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("Industry"))

    # Check if column exists after null operations
    if "DUNS Number" not in df.columns:
        print(
            "Warning: Column 'DUNS Number' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(df.schema["DUNS Number"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(initcap(trim(col("DUNS Number"))).alias("DUNS Number"))
        elif isinstance(df.schema["DUNS Number"].dataType, (IntegerType, FloatType, DoubleType, LongType, ShortType)):
            transformed_columns.append(col("DUNS Number"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("DUNS Number"))

    # Check if column exists after null operations
    if "Global Ultimate DUNS" not in df.columns:
        print(
            "Warning: Column 'Global Ultimate DUNS' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(df.schema["Global Ultimate DUNS"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(initcap(trim(col("Global Ultimate DUNS"))).alias("Global Ultimate DUNS"))
        elif isinstance(
            df.schema["Global Ultimate DUNS"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("Global Ultimate DUNS"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("Global Ultimate DUNS"))

    # Check if column exists after null operations
    if "Domestic Ultimate DUNS" not in df.columns:
        print(
            "Warning: Column 'Domestic Ultimate DUNS' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(df.schema["Domestic Ultimate DUNS"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(initcap(trim(col("Domestic Ultimate DUNS"))).alias("Domestic Ultimate DUNS"))
        elif isinstance(
            df.schema["Domestic Ultimate DUNS"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("Domestic Ultimate DUNS"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("Domestic Ultimate DUNS"))

    # Check if column exists after null operations
    if (
        "SiteEmployee#"
        not in df.columns
    ):
        print(
            "Warning: Column 'SiteEmployee#' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(
            df.schema["SiteEmployee#"].dataType,
            StringType
        ):
            # Add the transformed column to the list with alias
            transformed_columns.append(
                initcap(
                    trim(
                      col(
                        "SiteEmployee#"
                      )
                    )
                  )\
                  .alias(
                  "SiteEmployee#"
                )
            )
        elif isinstance(
            df.schema["SiteEmployee#"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(
                col(
                  "SiteEmployee#"
                )
            )
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(
                col(
                  "SiteEmployee#"
                )
            )

    # Check if column exists after null operations
    if (
        "GlobalEmployee#"
        not in df.columns
    ):
        print(
            "Warning: Column 'GlobalEmployee#' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(
            df.schema["GlobalEmployee#"].dataType,
            StringType
        ):
            # Add the transformed column to the list with alias
            transformed_columns.append(
                initcap(
                    trim(
                      col(
                        "GlobalEmployee#"
                      )
                    )
                  )\
                  .alias(
                  "GlobalEmployee#"
                )
            )
        elif isinstance(
            df.schema["GlobalEmployee#"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(
                col(
                  "GlobalEmployee#"
                )
            )
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(
                col(
                  "GlobalEmployee#"
                )
            )

    # Check if column exists after null operations
    if "OrigMaxSegment" not in df.columns:
        print(
            "Warning: Column 'OrigMaxSegment' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(df.schema["OrigMaxSegment"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(initcap(trim(col("OrigMaxSegment"))).alias("OrigMaxSegment"))
        elif isinstance(df.schema["OrigMaxSegment"].dataType, (IntegerType, FloatType, DoubleType, LongType, ShortType)):
            transformed_columns.append(col("OrigMaxSegment"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("OrigMaxSegment"))

    df = df.select(
        *[
          col(c)
          for c in df.columns
          if (
          c
          not in ["Firm",  "TotalEmp",  "SICCode",  "MaxSegment",  "NumEmployeeStrata",  "Industry",  "DUNS Number",              "Global Ultimate DUNS",  "Domestic Ultimate DUNS",              "SiteEmployee#",              "GlobalEmployee#",              "OrigMaxSegment"]
        )
        ],
        *transformed_columns
    )

    return df
