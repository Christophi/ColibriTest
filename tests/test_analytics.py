from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def test_anomaly_detection():
    spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    
    # Create test data with multiple normal turbines and one clearly anomalous turbine
    data = []
    
    # Add 5 normal turbines with consistent power around 3.0
    for turbine_id in range(1, 6):
        data.append((turbine_id, "2099-01-01 00:00:00", 12.0, 180.0, 3.0, "test.csv", False, False, "2099-01-01"))
        data.append((turbine_id, "2099-01-01 01:00:00", 13.0, 190.0, 3.0, "test.csv", False, False, "2099-01-01"))
    
    # Add one anomalous turbine with much higher power output
    data.append((99, "2099-01-01 00:00:00", 12.0, 180.0, 10.0, "test.csv", False, False, "2099-01-01"))
    data.append((99, "2099-01-01 01:00:00", 13.0, 190.0, 10.0, "test.csv", False, False, "2099-01-01"))
    
    columns = ["turbine_id", "timestamp", "wind_speed", "wind_direction", "power_output",
               "source_file", "has_imputed_values", "has_outlier_correction", "date"]
    silver_df = spark.createDataFrame(data, columns)
    
    # RUn the analytics code
    from src.analytics import calculate_summary_statistics, detect_anomalous_turbines
    stats_df = calculate_summary_statistics(silver_df)
    result = detect_anomalous_turbines(silver_df, stats_df)
    
    # Check that the anomalous turbine was detected
    anomalies_df = result["df"]
    assert anomalies_df.count() > 0
    
    # Verify it was turbine 99 that was flagged
    assert anomalies_df.filter(F.col("turbine_id") == 99).count() > 0
    
    spark.stop()