import hopsworks
import sys
sys.path.append("src")
from logger import logging as logger

def clean():
    project=hopsworks.login(
    api_key_value="ROE3M12HVdFHSkfq.ySguQ4M8L2Rz5atWcE8oK0RLD6rGDg3uVsCTwCfuZyr4N30poJNEuUpQ3hwZRx5C",
    project="rohanpatankar"
    )
    feature_store=project.get_feature_store()
    logger.info("Deleting the featire views and datasets")
    try:
        feature_view=feature_store.get_feature_views(name="energy_forecasting_view")
        for feat_view in feature_view:
            try:
                feat_view.delete()
            except Exception as e:
                raise e
    except Exception as e:
        raise e
    logger.info("Deleting the feature_group")

    try:
        feature_group=feature_store.get_feature_groups(name="energy_forecasting")
        for feat_group in feature_group:
            try:
                feat_group.delete()
            except Exception as e:
                raise e
    except Exception as e:
        raise e

if __name__=="__main__":
    clean()