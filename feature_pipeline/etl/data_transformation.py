import pandas as pd
import sys
sys.path.append("src")
from logger import logging as logger

class Transformation(object):
    @staticmethod
    def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
        """
        Rename columns to match our schema.
        """
        data = df.copy()
        # Drop irrelevant columns.
        data.drop(columns=["HourDK"], inplace=True)
        # Rename columns
        logger.info("Renaming columns...")
        data.rename(
            columns={
                "HourUTC": "datetime_utc",
                "PriceArea": "area",
                "ConsumerType_DE35": "consumer_type",
                "TotalCon": "energy_consumption",
            },
            inplace=True,
        )
        logger.info("rename cols successfully done...")
        return data

    @staticmethod
    def cast_columns(df: pd.DataFrame) -> pd.DataFrame:
        """
        Cast columns to the correct data type.
        """
        logger.info("Typecasting the column values...")
        data = df.copy()
        data["datetime_utc"] = pd.to_datetime(data["datetime_utc"])
        data["area"] = data["area"].astype("string")
        data["consumer_type"] = data["consumer_type"].astype("int32")
        data["energy_consumption"] = data["energy_consumption"].astype("float64")
        logger.info("Typecasting the column done...")
        return data

    @staticmethod
    def encode_area_column(df: pd.DataFrame) -> pd.DataFrame:
        """
        Encode the area column to integers.
        """
        logger.info("Simple label encoding on area mapping feature...")
        data = df.copy()
        area_mappings = {"DK": 0, "DK1": 1, "DK2": 2}
        data["area"] = data["area"].map(lambda string_area: area_mappings.get(string_area))
        data["area"] = data["area"].astype("int8")
        logger.info("label encoding on area mapping feature done...")
        return data
