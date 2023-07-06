import json
import logging
from pathlib import Path
import sys,os
sys.path.append(os.getcwd())

def get_logger(name:str)->logging.Logger:
     """
    Template for getting a logger.

    Args:
        name: Name of the logger.

    Returns: Logger.
    """
     logging.basicConfig(level=logging.INFO)
     logger=logging.getLogger(name=name)

     return logger
