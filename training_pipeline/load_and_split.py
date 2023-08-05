import hopsworks
import pandas as pd
import wandb
from sktime.forecasting.model_selection import temporal_train_test_split
from utils import init_wandb_run

def load_dataset_from_feature_store(
      feature_view_version,training_dataset_version  
):
    project=hopsworks.login(
    api_key_value="ROE3M12HVdFHSkfq.ySguQ4M8L2Rz5atWcE8oK0RLD6rGDg3uVsCTwCfuZyr4N30poJNEuUpQ3hwZRx5C",
    project="rohanpatankar"
    )
    fs = project.get_feature_store()

    with init_wandb_run(name="load_training_data",job_type="load_feature_view",group="dataset") as run:
        feature_view=fs.get_feature_view(
            name="energy_forecasting_view",
            version=feature_view_version
        )
        data,_=feature_view.get_training_data(training_dataset_version=training_dataset_version)
        fv_metadata=feature_view.to_dict()
        # 1.query
        # 2.features
        # 3.link
        # 4.feature_view_version
        # 5. training_dataset_version
        fv_metadata["query"]=fv_metadata["query"].to_string()
        fv_metadata["features"]=[f.name for f in fv_metadata["features"]]
        fv_metadata["link"]=feature_view._feature_view_engine._get_feature_view_url(feature_view)
        fv_metadata["feature_view_version"]=feature_view_version
        fv_metadata["training_dataset_version"]=training_dataset_version
        artifacts_wandb=wandb.Artifact(
            name="forecasting_project_artifacts",
            type="feature_view",
            metadata=fv_metadata
        )
        run.log_artifact(artifacts_wandb)

        run.finish()
    
    with init_wandb_run(name="train_test_split",job_type="prepare_dataset",group="dataset") as run:
        run.use_artifact("forecasting_project_artifacts:latest")
        y_train, y_test, X_train, X_test=prepare_data(data=data)
        for split in ["train","test"]:
            split_X=locals()[f"X_{split}"]
            split_y = locals()[f"y_{split}"]
            split_metadata = {
                "timespan": [
                    split_X.index.get_level_values(-1).min(),
                    split_X.index.get_level_values(-1).max(),
                ],
                "dataset_size": len(split_X),
                "num_areas": len(split_X.index.get_level_values(0).unique()),
                "num_consumer_types": len(split_X.index.get_level_values(1).unique()),
                "y_features": split_y.columns.tolist(),
                "X_features": split_X.columns.tolist(),
            }

            artifact = wandb.Artifact(
                name=f"split_{split}",
                type="split",
                metadata=split_metadata,
            )
            run.log_artifact(artifact)
        run.finish()
    return y_train, y_test, X_train, X_test

def prepare_data(
        data,target="energy_consumption"
):
    """
    Structure the data for training:
    - Set the index as is required by sktime.
    - Prepare exogenous(independent) variables.
    - Prepare the time series to be forecasted.
    - Split the data into train and test sets.
    """
    data["datetime_utc"] = pd.PeriodIndex(data["datetime_utc"], freq="H")
    data = data.set_index(["area", "consumer_type", "datetime_utc"]).sort_index()

    # Prepare exogenous(independent) variables.
    X = data.drop(columns=[target])
    # Prepare the time series to be forecasted.
    y = data[[target]]

    y_train, y_test, X_train, X_test = temporal_train_test_split(y, X, test_size=fh)

    return y_train, y_test, X_train, X_test
