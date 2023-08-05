import wandb
import pandas as pd
import joblib

APIKEY_WANDB="2096548cf410d5d313351f72194a9db26247ea58"
wandb_project="forecasting-mlops"
project_entity="a988"

def init_wandb_run(
        project,entity,name,group=None,job_type=None,add_timestamp_to_name=False,run_id=None,resume=None,reinit=False,
):
    if add_timestamp_to_name:
        name = f"{name}_{pd.Timestamp.now().strftime('%Y-%m-%d_%H-%M-%S')}"
    run = wandb.init(
        project=project,
        entity=entity,
        name=name,
        group=group,
        job_type=job_type,
        id=run_id,
        reinit=reinit,
        resume=resume,
    )
    return run

def save_model(model, model_path):
    """
    Template for saving a model.

    Args:
        model: Trained model.
        model_path: Path to save the model.
    """
    joblib.dump(model, model_path)


def load_model(model_path):
    """
    Template for loading a model.

    Args:
        model_path: Path to the model.

    Returns: Loaded model.
    """

    return joblib.load(model_path)


def check_if_artifact_exists(
        artifact_name,project,entity
):
    try:
        get_artifact(artifact_name,project,entity)
        return True
    except wandb.errors.CommError:
        return False


def get_artifact(artifact_name,project,entity):
    api=wandb.Api()
    artifacts=api.artifact(f"{entity}/{project}/{artifact_name}:latest")
    return artifacts