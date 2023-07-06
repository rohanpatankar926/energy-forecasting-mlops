# ETL Pipeline



This repository contains the files for an Extract, Transform, and Load (ETL) pipeline.

# Data API we have used
<a href="https://www.energidataservice.dk/tso-electricity/ConsumptionDE35Hour">https://www.energidataservice.dk/tso-electricity/ConsumptionDE35Hour</a>

## File Structure

The directory structure is as follows:
-  `feature_pipeline`: The main directory for the data pipeline project
    - `pipeline_main.py`: The main script that executes the ETL pipeline.
    - `etl/` (directory):
      - `__init__.py`: An empty file that marks the `etl` directory as a Python package.
      <!-- - `data_load.py`: Contains functions for loading data into the pipeline. -->
      - `data_ingestion.py`: Contains functions for ingesting data from a source.
      - `data_transformation.py`: Includes functions for transforming the ingested data.
       - `data_validation.py`: Includes functions for validating the loaded data using expectation suite.
- `src`:The logger and exception scripts are stored here
    - `logger.py`: For capturing datetime logs
    - `exception.py`: For capturing our custom exceptions 
## Usage

To execute the ETL pipeline, run the `pipeline_main.py` script. Ensure that all the necessary dependencies are installed and properly configured before running the script.

Modify the various files within the `etl/` directory as per your specific data extraction, transformation, and loading requirements.

## Dependencies

List any external dependencies or libraries required to run the ETL pipeline. Include installation instructions if necessary.

