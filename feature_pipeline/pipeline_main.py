import pandas as pd
from etl import data_ingestion,data_transformation
import sys
sys.path.append("src")
from logger import logging as logger

def transform_data(data: pd.DataFrame):
    rename_cols_data=data_transformation.Transformation.rename_columns(data)
    type_casted_data=data_transformation.Transformation.cast_columns(rename_cols_data)
    encoded_final_data=data_transformation.Transformation.encode_area_column(type_casted_data)
    return encoded_final_data

def run(export_end_reference_datetime=None,days_delay=15,days_export=30,url="https://api.energidataservice.dk/dataset/ConsumptionDE35Hour"):
    logger.info(f"Data extraction from {url} started...")
    data,metadata=data_ingestion.Extract.from_api(export_end_reference_datetime=export_end_reference_datetime,days_delay=days_delay,days_export=days_export,url=url)
    logger.info(f"Data transformation from the extracted data started...")
    data_transformation=transform_data(data=data)
    return data_transformation


if __name__=="__main__":
    print(run())