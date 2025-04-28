from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
import json
from datetime import datetime
import os
import logging

def handle_missing_values(df):
    """
    Detect missing values in the dataset and impute them according to 2
    different imputation strategies 
    """

    # Get total source count for use in reporting later
    total_rows = df.count()
    missing_counts = {}
    
    # Get missing counts for reporting and logging
    for col in ["power_output", "wind_speed", "wind_direction"]:
        missing_count = df.filter(F.col(col).isNull()).count()
        missing_percentage = (missing_count / total_rows) * 100
        missing_counts[col] = missing_count
        logging.info(f"Column {col}: {missing_count} missing values ({missing_percentage:.2f}%)")

    # Start a tracking column for imputed values
    result_df = df.withColumn("has_imputed_values", F.lit(False))

    # Define window for time-based operations by turbine_id
    w_by_turbine_time = Window.partitionBy("turbine_id").orderBy("timestamp")

    # Track imputation methods used
    # Using set as we only need a distinct list
    imputation_methods_used = set()

    # For missing values, impute using 2 possible strategies:
    # 1: Use average value of before and after missing value, if they exist
    # 2: Use average of the wind turbine
    for col in ["power_output", "wind_speed", "wind_direction"]:
        if missing_counts[col] > 0:
            logging.info(f"Imputing missing values for {col}")
            

        # 1. Closest value 
        result_df = result_df.withColumn(
            f"{col}_prev", F.lag(col).over(w_by_turbine_time)
        ).withColumn(
            f"{col}_next", F.lead(col).over(w_by_turbine_time)
        )

        # Flag rows to be imputed
        result_df = result_df.withColumn(
            "will_interpolate",
            F.col(col).isNull() & F.col(f"{col}_prev").isNotNull() & F.col(f"{col}_next").isNotNull()
        )

        # Update missing value with interpolated value
        result_df = result_df.withColumn(
            col,
            F.when(
                F.col("will_interpolate"),
                (F.col(f"{col}_prev") + F.col(f"{col}_next")) / 2
            ).otherwise(F.col(col)))

        # Update tracking flag
        result_df = result_df.withColumn(
            "has_imputed_values",
            F.when(
                F.col("will_interpolate"), 
                F.lit(True)
            ).otherwise(F.col("has_imputed_values"))
        )

        # Check if this method was used (for reporting)
        interpolation_count = result_df.filter(F.col("will_interpolate") == True).count()
        if interpolation_count > 0:
            imputation_methods_used.add("interpolation")
        
        # Drop temporary columns
        result_df = result_df.drop(f"{col}_prev", f"{col}_next", "will_interpolate")

        # 2. For any remaining missing values, use the average of the turbine
        # First, get the avergaes per turbine
        turbine_avgs = result_df.groupBy("turbine_id").agg(
            F.avg(col).alias(f"{col}_turbine_avg")
        )

        # Join back the turbine averages
        result_df = result_df.join(turbine_avgs, on="turbine_id")
        
        # Flag rows that will use turbine average
        result_df = result_df.withColumn(
            "will_use_turbine_avg",
            F.col(col).isNull() & F.col(f"{col}_turbine_avg").isNotNull()
        )
        
        # Update the value
        result_df = result_df.withColumn(
            col,
            F.when(
                F.col("will_use_turbine_avg"),
                F.col(f"{col}_turbine_avg")
            ).otherwise(F.col(col))
        )

        # Update tracking flag
        result_df = result_df.withColumn(
            "has_imputed_values",
            F.when(
                F.col("will_use_turbine_avg"), 
                F.lit(True)
            ).otherwise(F.col("has_imputed_values"))
        )
        
        # Check if this method was used
        turbine_avg_count = result_df.filter(F.col("will_use_turbine_avg") == True).count()
        if turbine_avg_count > 0:
            imputation_methods_used.add("turbine_average")
        
        # Drop temporary columns
        result_df = result_df.drop(f"{col}_turbine_avg", "will_use_turbine_avg")   


    # Count total imputed values
    imputed_count = result_df.filter(F.col("has_imputed_values") == True).count()
    
    # Generate report (to be appended to the full silver report later)
    quality_report = {
            "timestamp": datetime.now().isoformat(),
            "operation": "missing_value_imputation",
            "records_processed": total_rows,
            "data_quality": {
                "initial_missing_values": missing_counts,
                "total_imputed_records": imputed_count,
                "imputation_percentage": (imputed_count / total_rows) * 100 if total_rows > 0 else 0
            },
            "methods_used": list(imputation_methods_used)
        }

    return {
        "df": result_df,
        "quality_report": quality_report
    }      


def handle_outliers(df):
    """
    Since we are working with a time series it may be better to impute outliers as well
    rather than simply removing them. 
    """

    # Get count for reporting later
    total_rows = df.count()

    # Add a column to track which rows need correcting
    result_df = df.withColumn("has_outlier_correction", F.lit(False))

    # Track outlier counts by column and turbine
    outlier_counts = {}
    
    # For each column, detect and handle outliers
    for col in ["power_output", "wind_speed", "wind_direction"]:
        logging.info(f"Cleaning outliers in {col}")
        
        # Calculate turbine-specific statistics
        turbine_stats = result_df.groupBy("turbine_id").agg(
            F.mean(col).alias("mean"),
            F.stddev(col).alias("stddev")
        )   

        # Join statistics back to main data
        result_df = result_df.join(
            turbine_stats.select("turbine_id", "mean", "stddev"),
            on="turbine_id"
        )

        # Calculate number of std deviations
        result_df = result_df.withColumn(
            "num_stddevs",
            F.when(
                F.col("stddev") > 0,  # Avoid division by zero
                F.abs((F.col(col) - F.col("mean")) / F.col("stddev"))
            ).otherwise(F.lit(0))
        )

        # Since an anomaly is considered an output outside of 2 std deviations
        # anything outside 3 deviations is almost certainly an 'outlier'
        result_df = result_df.withColumn(
            "is_outlier",
            F.col("num_stddevs") > 3 
        )

        # Count outliers for reporting
        outlier_count = result_df.filter(F.col("is_outlier") == True).count()
        outlier_counts[col] = outlier_count
        
        # Define window for calculating rolling median
        w_rolling = Window.partitionBy("turbine_id").orderBy("timestamp").rowsBetween(-3, 3) 

        # Calculate rolling median for better replacement value
        result_df = result_df.withColumn(
            "rolling_median", 
            F.expr(f"percentile_approx({col}, 0.5)").over(w_rolling)
        )
        
        # Replace outliers with the rolling median or mean
        result_df = result_df.withColumn(
            col,
            F.when(
                F.col("is_outlier") == True,
                F.coalesce(F.col("rolling_median"), F.col("mean")) # Coalescing in case median is null for any reason
            ).otherwise(F.col(col))
        ) 

        # Update outlier correction tracking
        result_df = result_df.withColumn(
            "has_outlier_correction",
            F.when(
                F.col("is_outlier") == True,
                F.lit(True)
            ).otherwise(F.col("has_outlier_correction"))
        )        

        # Drop temporary columns
        result_df = result_df.drop("mean", "stddev", "num_stddevs", "is_outlier", "rolling_median")


    # Count total corrections made
    corrected_count = result_df.filter(F.col("has_outlier_correction") == True).count()

    # Create the outlier report
    quality_report = {
            "timestamp": datetime.now().isoformat(),
            "operation": "outlier_cleaning",
            "records_processed": total_rows,
            "data_quality": {
                "outliers_detected": outlier_counts,
                "total_corrected_records": corrected_count,
                "correction_percentage": (corrected_count / total_rows) * 100 if total_rows > 0 else 0
            }
        }
            
    return {
        "df": result_df,
        "quality_report": quality_report
    }



def clean_data(bronze_df):
    """
    Master function for detecting missing values, outliers and then writing
    the final report to a JSON
    """

    # Missing values
    missing_result = handle_missing_values(bronze_df)
    imputed_df = missing_result["df"]
    missing_report = missing_result["quality_report"]
    
    # Outliers
    outlier_result = handle_outliers(imputed_df)
    cleaned_df = outlier_result["df"]
    outlier_report = outlier_result["quality_report"]

    # Add a date field at the end for use in analytics.py/gold stage
    silver_df = cleaned_df.withColumn("date", F.to_date("timestamp"))

    combined_report = {
        "timestamp": datetime.now().isoformat(),
        "operation": "silver_data_cleaning",
        "performance": {
            "records_processed": bronze_df.count()
        },
        "reports": {
            "missing_values": missing_report,
            "outliers": outlier_report
        }
    }

    # Write final report which concatenates all 3 jsons generated in this stage 
    report_path = f"reports/silver_cleaning_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_path, "w") as f:
        json.dump(combined_report, f, indent=2)
    
    return {
        "df": silver_df,
        "quality_report": combined_report
    }

