import sqlite3
import os
import logging

def initialise_database():
    """
    Initialise the SQLite database with required tables.
    Creates the database file and tables if they don't exist
    """
    # Ensure database directory exists
    db_dir = "data/database"
    os.makedirs(db_dir, exist_ok=True)
    
    db_path = f"{db_dir}/turbine_data.db"
    logging.info(f"Initialising database at {db_path}")
    
    # Connect to database (creates it if it doesn't exist)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create cleaned_turbine_data table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS cleaned_turbine_data (
        turbine_id INTEGER,
        timestamp TEXT,
        wind_speed REAL,
        wind_direction REAL,
        power_output REAL,
        source_file TEXT NOT NULL,
        has_imputed_values INTEGER NOT NULL,
        has_outlier_correction INTEGER NOT NULL,
        date TEXT,
        PRIMARY KEY (turbine_id, timestamp)
    )
    ''')
    
    # Create turbine_statistics table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS turbine_statistics (
        turbine_id INTEGER,
        date TEXT,
        turbine_daily_min_power REAL,
        turbine_daily_max_power REAL,
        turbine_daily_avg_power REAL,
        turbine_daily_stddev_power REAL,
        reading_count INTEGER NOT NULL,
        PRIMARY KEY (turbine_id, date)
    )
    ''')
    
    conn.commit()
    conn.close()
    logging.info("Database initialised successfully")
    
    return db_path

def save_to_database(df, table_name):
    # Ensure database directory exists
    db_dir = "data/database"    
    db_path = f"{db_dir}/turbine_data.db"

    if not os.path.exists(db_path):
        initialise_database() 
    
    try:
        # Convert to pandas (required for SQLite)
        pandas_df = df.toPandas()
        
        # Connect to database
        conn = sqlite3.connect(db_path)
        
        # Convert boolean columns to integers for SQLite compatibility
        if 'has_imputed_values' in pandas_df.columns:
            pandas_df['has_imputed_values'] = pandas_df['has_imputed_values'].astype(int)
        if 'has_outlier_correction' in pandas_df.columns:
            pandas_df['has_outlier_correction'] = pandas_df['has_outlier_correction'].astype(int)
        
        # Save to database (will create table if it doesn't exist)
        # For production use I would take an approach that uses a primary key and checks for existence/updates accordingly
        # For this task I will simply replace the data 
        pandas_df.to_sql(table_name, conn, if_exists='replace', index=False)
        
        conn.close()
        logging.info(f"Successfully saved to table: {table_name}")
        return True
        
    except Exception as e:
        logging.error(f"Error saving to database: {str(e)}")
        return False