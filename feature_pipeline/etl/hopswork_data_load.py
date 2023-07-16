import pandas as pd
import hopsworks
from hsfs.feature_group import FeatureGroup
from great_expectations.core import ExpectationSuite, ExpectationConfiguration

def to_feature_store(
        data:pd.DataFrame,
        validation_expectation: ExpectationSuite,
        feautures_store_group_version: int=1
):
    project=hopsworks.login(
        api_key_value="ROE3M12HVdFHSkfq.ySguQ4M8L2Rz5atWcE8oK0RLD6rGDg3uVsCTwCfuZyr4N30poJNEuUpQ3hwZRx5C",
        project="rohanpatankar"
    )
    feature_store=project.get_feature_store()
    
    # Create feature group.
    energy_feature_group = feature_store.get_or_create_feature_group(
        name="energy_forecasting_final",
        version=feautures_store_group_version,
        description="Denmark hourly energy consumption data. Data is uploaded with an 15 days delay.",
        primary_key=["area", "consumer_type"],
        event_time="datetime_utc",
        expectation_suite=validation_expectation,
    )
    #uploading the data to hopsworks
    energy_feature_group.insert(
        features=data,
        overwrite=False,
        write_options={
            "wait_for_job": True,
        },
    )
    
    feature_descriptions = [
        {
            "name": "datetime_utc",
            "description": """
                            Datetime interval in UTC when the data was observed.
                            """,
            "validation_rules": "Always full hours, i.e. minutes are 00",
        },
        {
            "name": "area",
            "description": """
                            Denmark is divided in two price areas, divided by the Great Belt: DK1 and DK2.
                            If price area is “DK”, the data covers all Denmark.
                            """,
            "validation_rules": "0 (DK), 1 (DK1) or 2 (Dk2) (int)",
        },
        {
            "name": "consumer_type",
            "description": """
                            The consumer type is the Industry Code DE35 which is owned by Danish Energy. 
                            The code is used by Danish energy companies.
                            """,
            "validation_rules": ">0 (int)",
        },
        {
            "name": "energy_consumption",
            "description": "Total electricity consumption in kWh.",
            "validation_rules": ">=0 (float)",
        },
    ]

    for desc in feature_descriptions:
        energy_feature_group.update_feature_description(
            desc["name"], desc["description"]
        )
    # Update statistics.
    energy_feature_group.statistics_config = {
        "enabled": True,
        "histograms": True,
        "correlations": True,
    }
    energy_feature_group.update_statistics_config()
    energy_feature_group.compute_statistics()
    return energy_feature_group