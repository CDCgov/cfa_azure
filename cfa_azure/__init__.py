import datetime
import logging
import os
import sys

from cfa_azure import helpers

__all__ = ["batch", "helpers", "clients"]

logger = logging.getLogger(__name__)
run_time = datetime.datetime.now()
now_string = f"{run_time:%Y-%m-%d_%H:%M:%S%z}"
# Logging
if not os.path.exists("logs"):
    os.mkdir("logs")
logfile = os.path.join("logs", f"{now_string}.log")
FORMAT = "[%(levelname)s] %(asctime)s: %(message)s"

logging.basicConfig(
    level=helpers.get_log_level(),
    format=FORMAT,
    datefmt="%Y-%m-%d_%H:%M:%S%z",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(logfile),
    ],
)
