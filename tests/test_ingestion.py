import os
import pandas as pd
from pyspark.sql import SparkSession

def test_ingest_data():

    spark = SparkSession.builder.appName("test").master("local[1]").getOrCreate()
    
    # Create test data directory and files
    os.makedirs("data/source", exist_ok=True)
    
    # Create a simple test csv
    test_data = pd.DataFrame({
        "timestamp": ["2022-01-01 00:00:00", "2022-01-01 01:00:00"],
        "turbine_id": [1, 1],
        "wind_speed": [12.0, 13.0],
        "wind_direction": [180.0, 190.0],
        "power_output": [3.0, 3.5]
    })
    test_data.to_csv("data/source/test_data.csv", index=False)
    
    # Run the ingest_data function
    from src.ingestion import ingest_data
    result_df = ingest_data(spark)
    
    # Check that data was loaded
    assert result_df is not None
    assert result_df.count() > 0
    assert "source_file" in result_df.columns

    spark.stop()