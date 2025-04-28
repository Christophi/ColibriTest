## Installation and run:
- Clone the repository
- Install the requirements in requirements.txt
- run with `python src/main.py` or `python3 src/main.py`

## Testing:
- Run the unit tests with `python -m pytest tests/`

## Pipeline:
`Source → Bronze → Silver → Gold → Database`
Source: Raw CSV files
Bronze: Concatenated data in parquet format
Silver: Cleaned data, with missing values and outliers addressed
Gold: Summary statistics, Anomaly detection results 
Database: Holds the above two datasets in a SQLite database

## Code Overview
`ingestion.py` (Bronze): Concatenates and loads data with schema validation.

`cleaning.py` (Silver): Two-stage data cleaning (missing values, outliers).

`analytics.py` (Gold): Summary statistics and anomaly detection.

`database.py` (Database): Table creation and save to database function.

`main.py` (Orchestrator): Pipeline orchestration.

`utils.py`: Common functions.

## Description 
This is my implementation of the pipeline that cleans and analyses data for wind turbines. I have used a basic medallion architecture to persist the data at each stage, alongside JSON reports found in `reports/` to summarise changes made and issues found. 

There are 3 stages of data processing, with a final stage that saves the output to two tables in a SQLite database: `cleaned_turbine_data` and `turbine_statistics`. Running `main.py` will show the contents of these tables.

### Technology
As the task specified that the focus would be on the clarity of code rather than the overall design of the system, I have decided to go for a simple approach that meets the requirements of the spec. 

The data is small and my first thought was to use Pandas, which would have been appropriate for processing data of this size, but since the task asked to focus on scalability I have decided to go with PySpark. However, there is a conversion to pandas before writing to the database.

Other than PySpark, Pytest, and SQLite3, I have avoided using any other packages for the sake of simplciity. 


## Test data additions
Upon analysis I could not find any missing values or outliers in the data provided, so I have generated data to fulfill these test cases which allowed me to test my code. They can be found in `data/source/`. 

`data_group_missing_values.csv` has 5 missing values:
- wind_speed: 2
- wind_direction: 2
- power_output: 1

`data_group_outlier.csv` has 2 outliers, 1 for `power_output` and 1 for `wind_speed`

`data_group_anomalies.csv` contains data that has 1 anomaly:
- I use 3 standard deviations for an outlier and 2 for an anomaly, so to avoid an overlap in the reporting I have made sure the anomalies will not be reported as a outliers (between 2 and 3 deviations) and won't get imputed before the gold stage
- In a productionised build we would need to clarify the difference between an 'outlier' and an 'anomaly' in the requirements to avoid this edge case

## Considerations for further work
If we were going into production and given more time, I would:
- Spend more time on the orchestration, giving each stage its own error catching rather than using a single try/except block.
- Use a more sophisticated approach to the reporting of outliers/missing values, rather than outputting a simple JSON report.
- Make some hard-coded values configurable, for example the number of standard deviations that determines an outlier or the range for the rolling median window used during imputation.