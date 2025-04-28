from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def test_handle_missing_values():

    spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    
    # Create test data with missing values
    data = [
        (1, "2022-01-01 00:00:00", 12.0, 180.0, 3.0, "test.csv"),
        (1, "2022-01-01 01:00:00", None, 190.0, 3.5, "test.csv"),
        (1, "2022-01-01 02:00:00", 14.0, None, 4.0, "test.csv")
    ]
    columns = ["turbine_id", "timestamp", "wind_speed", "wind_direction", "power_output", "source_file"]
    df = spark.createDataFrame(data, columns)
    df = df.withColumn("timestamp", F.to_timestamp("timestamp"))
    
    # Import and run missing values code
    from src.cleaning import handle_missing_values
    result = handle_missing_values(df)
    
    # Check that missing values were handled
    result_df = result["df"]
    assert result_df.filter(F.col("wind_speed").isNull()).count() == 0
    assert result_df.filter(F.col("wind_direction").isNull()).count() == 0
    
    spark.stop()

def test_handle_outliers():

    spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    
    # Create standard test data
    data = []
    for i in range(20):
        data.append((1, f"2022-01-01 {i:02d}:00:00", 10.0, 180.0, 3.0, "test.csv"))
    
    # Add one extreme outlier
    data.append((1, "2022-01-01 21:00:00", 1000.0, 180.0, 1000.0, "test.csv"))
    
    columns = ["turbine_id", "timestamp", "wind_speed", "wind_direction", "power_output", "source_file"]
    df = spark.createDataFrame(data, columns)
    df = df.withColumn("timestamp", F.to_timestamp("timestamp"))
    df = df.withColumn("has_imputed_values", F.lit(False))
    
    # Run the handle outliers code
    from src.cleaning import handle_outliers
    result = handle_outliers(df)
    
    # Check the outlier report to ensure the outliers were detected
    quality_report = result["quality_report"]["data_quality"]    
    assert quality_report["outliers_detected"]["wind_speed"] > 0
    assert quality_report["outliers_detected"]["power_output"] > 0
    
    # Check that the specific outlier row was corrected
    outlier_timestamp = "2022-01-01 21:00:00"
    outlier_row = result["df"].filter(F.col("timestamp") == outlier_timestamp).collect()
    assert outlier_row[0]["has_outlier_correction"] == True
    
    spark.stop()