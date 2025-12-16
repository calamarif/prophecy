from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pipeline101.config.ConfigStore import *
from pipeline101.functions import *

def data_cleaning_operations(spark: SparkSession, df: DataFrame) -> DataFrame:
    from pyspark.sql.functions import col, trim, regexp_replace, lower, upper, initcap
    from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType, LongType, ShortType
    # Remove rows where all columns are null
    df = df.na.drop(how = "all")
    # Step 2: Apply data cleansing operations
    # Start with the original columns
    transformed_columns = []

    # Check if column exists after null operations
    if "Account" not in df.columns:
        print("Warning: Column 'Account' not found after null operation. Skipping transformations for this column.")
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(df.schema["Account"].dataType, StringType):
            transformed_columns = [initcap(trim(col("Account"))).alias("Account")]
        elif isinstance(df.schema["Account"].dataType, (IntegerType, FloatType, DoubleType, LongType, ShortType)):
            transformed_columns = [col("Account")]
        else:
            transformed_columns = [col("Account")]

    # Check if column exists after null operations
    if "Address" not in df.columns:
        print("Warning: Column 'Address' not found after null operation. Skipping transformations for this column.")
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(df.schema["Address"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(initcap(trim(col("Address"))).alias("Address"))
        elif isinstance(df.schema["Address"].dataType, (IntegerType, FloatType, DoubleType, LongType, ShortType)):
            transformed_columns.append(col("Address"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("Address"))

    # Check if column exists after null operations
    if "Address Line 2" not in df.columns:
        print(
            "Warning: Column 'Address Line 2' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(df.schema["Address Line 2"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(initcap(trim(col("Address Line 2"))).alias("Address Line 2"))
        elif isinstance(df.schema["Address Line 2"].dataType, (IntegerType, FloatType, DoubleType, LongType, ShortType)):
            transformed_columns.append(col("Address Line 2"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("Address Line 2"))

    # Check if column exists after null operations
    if "City" not in df.columns:
        print("Warning: Column 'City' not found after null operation. Skipping transformations for this column.")
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(df.schema["City"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(initcap(trim(col("City"))).alias("City"))
        elif isinstance(df.schema["City"].dataType, (IntegerType, FloatType, DoubleType, LongType, ShortType)):
            transformed_columns.append(col("City"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("City"))

    # Check if column exists after null operations
    if "State" not in df.columns:
        print("Warning: Column 'State' not found after null operation. Skipping transformations for this column.")
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(df.schema["State"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(initcap(trim(col("State"))).alias("State"))
        elif isinstance(df.schema["State"].dataType, (IntegerType, FloatType, DoubleType, LongType, ShortType)):
            transformed_columns.append(col("State"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("State"))

    # Check if column exists after null operations
    if "ZipCode" not in df.columns:
        print("Warning: Column 'ZipCode' not found after null operation. Skipping transformations for this column.")
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(df.schema["ZipCode"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(initcap(trim(col("ZipCode"))).alias("ZipCode"))
        elif isinstance(df.schema["ZipCode"].dataType, (IntegerType, FloatType, DoubleType, LongType, ShortType)):
            transformed_columns.append(col("ZipCode"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("ZipCode"))

    # Check if column exists after null operations
    if "Country" not in df.columns:
        print("Warning: Column 'Country' not found after null operation. Skipping transformations for this column.")
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(df.schema["Country"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(initcap(trim(col("Country"))).alias("Country"))
        elif isinstance(df.schema["Country"].dataType, (IntegerType, FloatType, DoubleType, LongType, ShortType)):
            transformed_columns.append(col("Country"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("Country"))

    # Check if column exists after null operations
    if "MainPhoneNumber" not in df.columns:
        print(
            "Warning: Column 'MainPhoneNumber' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(df.schema["MainPhoneNumber"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(initcap(trim(col("MainPhoneNumber"))).alias("MainPhoneNumber"))
        elif isinstance(
            df.schema["MainPhoneNumber"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("MainPhoneNumber"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("MainPhoneNumber"))

    # Check if column exists after null operations
    if "Bookings in 2024" not in df.columns:
        print(
            "Warning: Column 'Bookings in 2024' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(df.schema["Bookings in 2024"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(initcap(trim(col("Bookings in 2024"))).alias("Bookings in 2024"))
        elif isinstance(
            df.schema["Bookings in 2024"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("Bookings in 2024"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("Bookings in 2024"))

    # Check if column exists after null operations
    if "Bookings in 2025" not in df.columns:
        print(
            "Warning: Column 'Bookings in 2025' not found after null operation. Skipping transformations for this column."
        )
    else:
        # If the column is a string type, apply text-based operations
        if isinstance(df.schema["Bookings in 2025"].dataType, StringType):
            # Add the transformed column to the list with alias
            transformed_columns.append(initcap(trim(col("Bookings in 2025"))).alias("Bookings in 2025"))
        elif isinstance(
            df.schema["Bookings in 2025"].dataType,
            (IntegerType, FloatType, DoubleType, LongType, ShortType)
        ):
            transformed_columns.append(col("Bookings in 2025"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("Bookings in 2025"))

    df = df.select(
        *[
          col(c)
          for c in df.columns
          if (
          c
          not in ["Account",  "Address",  "Address Line 2",  "City",  "State",  "ZipCode",  "Country",  "MainPhoneNumber",              "Bookings in 2024",  "Bookings in 2025"]
        )
        ],
        *transformed_columns
    )

    return df
