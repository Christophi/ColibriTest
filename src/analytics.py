from pyspark.sql import functions as F
import json
from datetime import datetime
import os
import logging

def calculate_summary_statistics(silver_df):
    """
    Returns an aggregated df for all turbines per day
    """

    return silver_df.groupBy("turbine_id", "date").agg(
        F.min("power_output").alias("turbine_daily_min_power"),
        F.max("power_output").alias("turbine_daily_max_power"),
        F.avg("power_output").alias("turbine_daily_avg_power"),
        F.stddev("power_output").alias("turbine_daily_stddev_power"),
        F.count("*").alias("reading_count")
    )


def detect_anomalous_turbines(silver_df, stats_df):
    """
    Function to identify anomalous turbines following the rule of any turbine over
    2 standard deviations from the mean.
    """

    # The spec does not specify whether to calculate the mean per turbine
    # or against the farm-wide mean. For simplicity I have implemented it against the
    # farm-wide mean per chosen time period (24 hours)
    daily_farm_stats = silver_df.groupBy("date").agg(
        F.avg("power_output").alias("farm_daily_avg"),
        F.stddev("power_output").alias("farm_daily_stddev")
    )

    # Since we already calculated the avg we can reuse the summary dataset for this join
    anomaly_df = stats_df.join(daily_farm_stats, on="date")

    # Calculate how many standard deviations each turbine is from the mean for the given time period
    anomaly_df = anomaly_df.withColumn(
        "deviation_from_mean", 
        F.abs(F.col("turbine_daily_avg_power") - F.col("farm_daily_avg"))
    ).withColumn(
        "standard_deviations",
        F.when(
            F.col("farm_daily_stddev") > 0,  # Avoid division by zero
            F.col("deviation_from_mean") / F.col("farm_daily_stddev")
        ).otherwise(F.lit(0))
    ).withColumn(
        "is_anomalous",
        F.col("standard_deviations") > 2 # As per the spec
    )

    # Filter to just anomalous turbines
    anomalous_turbines = anomaly_df.filter(F.col("is_anomalous") == True)

    # Get count for reporting
    anomaly_count = anomalous_turbines.count()

    # Use collect() to generate an array we can add to the JSON
    anomalous_turbines_and_timestamps = [{
        "turbine_id": row["turbine_id"], 
        "date": row["date"].isoformat() if hasattr(row["date"], "isoformat") else row["date"]
    } for row in anomalous_turbines.select("turbine_id", "date").distinct().collect()]

    # Create the JSON report
    anomaly_report = {
        "timestamp": datetime.now().isoformat(),
        "operation": "detect_anomalous_turbines",
        "num_anomalies_found": anomaly_count,
        "anomalous_cases": anomalous_turbines_and_timestamps
    }

    return {
        "df": anomalous_turbines,
        "anomaly_report": anomaly_report
    }



def process_gold_layer(silver_df):
    """
    Master function to run the gold layer functions and generate a report.
    """

    # Calculate summary statistics
    stats_df = calculate_summary_statistics(silver_df)
    
    # Detect anomalous turbines
    anomaly_result = detect_anomalous_turbines(silver_df, stats_df)
    anomalies_df = anomaly_result["df"]
    anomaly_report = anomaly_result["anomaly_report"]
    
    # Generate combined gold layer report    
    gold_report = {
        "timestamp": datetime.now().isoformat(),
        "operation": "gold_layer_processing",
        "summary_statistics": {
            "total_records": stats_df.count()
        },
        "anomaly_detection": anomaly_report
    }
    
    # Write gold layer report to file
    report_path = f"reports/gold_processing_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_path, "w") as f:
        json.dump(gold_report, f, indent=2)
        
    return {
        "stats_df": stats_df,
        "anomalies_df": anomalies_df,
        "report": gold_report
    }
