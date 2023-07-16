from datetime import datetime
from typing import Optional
import hopsworks
from utils import *
import hsfs
import sys
sys.path.append("src")
from logger import logging as logger

def create_feature_view(
    feature_group_version: Optional[int] = None,
    start_datetime: Optional[datetime] = None,
    end_datetime: Optional[datetime] = None,
) -> dict:
    if feature_group_version is None:
        feature_pipeline_metadata = load_json("feature_pipeline_metadata.json", load_dir="metadata_dir")
        feature_group_version = feature_pipeline_metadata["feature_group_version"]

    if start_datetime is None or end_datetime is None:
        feature_pipeline_metadata = load_json("feature_pipeline_metadata.json", load_dir="metadata_dir")
        start_datetime = datetime.strptime(
            feature_pipeline_metadata["export_datetime_utc_start"],
            feature_pipeline_metadata["datetime_format"],
        )
        end_datetime = datetime.strptime(
            feature_pipeline_metadata["export_datetime_utc_end"],
            feature_pipeline_metadata["datetime_format"],
        )

    project_login = hopsworks.login(
        api_key_value="ROE3M12HVdFHSkfq.ySguQ4M8L2Rz5atWcE8oK0RLD6rGDg3uVsCTwCfuZyr4N30poJNEuUpQ3hwZRx5C",
        project="rohanpatankar"
    )
    project = project_login.get_feature_store()
    try:
        feature_views = project.get_feature_views(name="energy_forecasting_view")
    except hsfs.client.exceptions.RestAPIError:
        logger.info("No feature views found for energy_consumption_denmark_view.")
        feature_views = []

    for feature_view in feature_views:
        try:
            feature_view.delete_all_training_datasets()
        except hsfs.client.exceptions.RestAPIError:
            logger.error(
                f"Failed to delete training datasets for feature view {feature_view.name} with version {feature_view.version}."
            )
        try:
            feature_view.delete()
        except hsfs.client.exceptions.RestAPIError:
            logger.error(
                f"Failed to delete feature view {feature_view.name} with version {feature_view.version}."
            )

    energy_consumption_fg = project.get_feature_group(
        "energy_forecasting_final", version=feature_group_version
    )
    ds_query = energy_consumption_fg.select_all()
    feature_view = project.create_feature_view(
        name="energy_forecasting_view",
        description="Energy consumption for Denmark forecasting model.",
        query=ds_query,
        labels=[],
    )

    logger.info(f"Creating training dataset between {start_datetime} and {end_datetime}.")
    feature_view.create_training_data(
        description="Energy consumption training dataset",
        data_format="csv",
        start_time=start_datetime,
        end_time=end_datetime,
        write_options={"wait_for_job": True},
    )

    metadata = {
        "feature_view_version": feature_view.version,
        "training_dataset_version": 1,
    }

    save_json(
        metadata,
        file_path="feature_view_metadata.json",
        save_dir="metadata_dir"
    )
    return metadata


if __name__ == "__main__":
    create_feature_view()
