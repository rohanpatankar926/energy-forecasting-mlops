import datetime
from json import JSONDecodeError
import pandas as pd
import requests
from yarl import URL
import os
import sys
sys.path.append("src")
from logger import logging as logger
from exception import CustomException
# logging.info("EXTRACTING THE DATA STARTED TO PERFORM")


class Extract(object):
    
    @classmethod
    def from_api(cls,export_end_reference_datetime=None,days_delay=15,days_export=30,url="https://api.energidataservice.dk/dataset/ConsumptionDE35Hour"):
        """ Extract data from the DK energy consumption API.

        Args:
            export_end_reference_datetime: The end reference datetime of the export window. If None, the current time is used.
                Because the data is always delayed with "days_delay" days, this date is used only as a reference point.
                The real extracted window will be computed as [export_end_reference_datetime - days_delay - days_export, export_end_reference_datetime - days_delay].
            days_delay: Data has a delay of 15 days. Thus, we have to shift our window with 15 days.
            days_export: The number of days to export.
            url: The URL of the API.

        Returns:
            A tuple of a Pandas DataFrame containing the exported data and a dictionary of metadata."""

        # Compute the export window.
        if export_end_reference_datetime is None:
            export_end_reference_datetime = datetime.datetime.utcnow().replace(
                minute=0, second=0, microsecond=0
            )
        else:
            export_end_reference_datetime = export_end_reference_datetime.replace(
                minute=0, second=0, microsecond=0
            )
        export_start = export_end_reference_datetime - datetime.timedelta(days=days_delay + days_export)
        export_end = export_end_reference_datetime - datetime.timedelta(days=days_delay)
        query_params = {
                "offset": 0,
                "sort": "HourUTC",
                "timezone": "utc",
                "start": export_start.strftime("%Y-%m-%dT%H:%M"),
                "end": export_end.strftime("%Y-%m-%dT%H:%M"),
            }
        
        url = URL(url) % query_params
        url=str(url)
        logger.info(f"Requesting data from API with URL: {url}")
        requested_data=requests.get(url)
        logger.info(f"Response received from API with status code: {requested_data.status_code} ")
        try:
            response=requested_data.json()
        except JSONDecodeError as e:
            raise e
        records = response["records"]
        records=pd.DataFrame(records)
        records.to_csv("a.csv")
        actual_dt_format="%Y-%m-%dT%:%M:%S"
        metadata={
        "days_delay":days_delay,
        "days_export":days_export,
        "export_datetime_utc_start": export_start.strftime(actual_dt_format),
        "export_datetime_utc_end": export_end.strftime(actual_dt_format),
        "datetime_format": actual_dt_format,
        }
        return records,metadata
    

if __name__=="__main__":
    print(Extract.from_api())