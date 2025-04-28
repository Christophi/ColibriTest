import os
import logging
from datetime import datetime
from pyspark.sql import SparkSession
import sqlite3

from database import initialise_database, save_to_database
from ingestion import ingest_data
from cleaning import clean_data
from analytics import process_gold_layer
from utils import save_dataframe

def configure_logging():
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    
    log_file = f"{log_dir}/pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

def main():
    # Set up logging
    configure_logging()
    logging.info("Starting wind turbine data processing pipeline")
    
    try:
        initialise_database()
        
        # Create Spark session
        spark = SparkSession.builder \
            .appName("TurbineDataProcessor") \
            .master("local[*]") \
            .getOrCreate()
        
        # BRONZE LAYER: Data Ingestion
        logging.info("Starting bronze layer: data ingestion")
        bronze_df = ingest_data(spark)
        save_dataframe(bronze_df, "bronze")
        
        # SILVER LAYER: Data Cleaning
        logging.info("Starting silver layer: data cleaning")
        silver_result = clean_data(bronze_df)
        silver_df = silver_result["df"] 
        save_dataframe(silver_df, "silver")
        
        # GOLD LAYER: Analytics
        logging.info("Starting gold layer: analytics")
        gold_result = process_gold_layer(silver_df)
        stats_df = gold_result["stats_df"]
        anomalies_df = gold_result["anomalies_df"]
        
        # Save gold data
        save_dataframe(stats_df, "gold/summary_statistics")
        if anomalies_df.count() > 0:
            save_dataframe(anomalies_df, "gold/anomalies")
            logging.info(f"Detected {anomalies_df.count()} anomalous turbines")
        else:
            logging.info("No anomalous turbines detected")
        
        # DATABASE STORAGE
        logging.info("Storing processed data in database")
        try:
            save_to_database(silver_df, "cleaned_turbine_data")
            save_to_database(stats_df, "turbine_statistics")
            if anomalies_df.count() > 0:
                save_to_database(anomalies_df, "anomalous_turbines")
            logging.info("Database storage completed successfully")
        except Exception as db_error:
            logging.error(f"Database storage error: {str(db_error)}")
        
        logging.info("Pipeline execution completed successfully")
        
    except Exception as e:
        logging.error(f"Pipeline error: {str(e)}", exc_info=True)
    finally:
        # Clean up Spark
        if 'spark' in locals():
            spark.stop()
            logging.info("Spark session stopped")

def debug_database():
    """Simple database query to check tables."""
    
    conn = sqlite3.connect("data/database/turbine_data.db")
    cursor = conn.cursor()
    
    print("\n=== Database Contents ===")
    
    # Query cleaned_turbine_data
    cursor.execute("SELECT * FROM cleaned_turbine_data LIMIT 15")
    print("\nTable: cleaned_turbine_data")
    columns = [description[0] for description in cursor.description]
    print("Columns:", columns)
    for row in cursor.fetchall():
        print(row)
    
    # Query turbine_statistics
    cursor.execute("SELECT * FROM turbine_statistics LIMIT 15")
    print("\nTable: turbine_statistics")
    columns = [description[0] for description in cursor.description]
    print("Columns:", columns)
    for row in cursor.fetchall():
        print(row)
    
    conn.close()


if __name__ == "__main__":
    main()
    debug_database()