from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, TimestampType, IntegerType, DoubleType
import pyspark.sql.functions as F
import os
from functools import reduce

# Define schema for turbine data
turbine_schema = StructType([
    StructField("timestamp", TimestampType(), False),
    StructField("turbine_id", IntegerType(), False),
    StructField("wind_speed", DoubleType(), True),
    StructField("wind_direction", DoubleType(), True),
    StructField("power_output", DoubleType(), True)
])

def ingest_data(spark):
    """
    Load data from CSV files with source filename tracking.
    """
    
    raw_data_path = 'data/source/'
    files = os.listdir(raw_data_path)

    print(files)

    all_dfs = []
    
    for file in files:
        # Read with defined schema
        file_df = spark.read.schema(turbine_schema).csv(raw_data_path+file, header=True)
        
        # Add filename as a column for traceability
        file_df = file_df.withColumn("source_file", F.lit(os.path.basename(file)))

        all_dfs.append(file_df)
    
    # Early return if no files processed
    if not all_dfs:
        return None
        
    # Otherwise use reduce to union the dataframes
    return reduce(lambda df1, df2: df1.unionByName(df2), all_dfs)