import pandas as pd
import requests
from copy import deepcopy
import os
import yaml
from datetime import datetime
import numpy as np
import importlib

from logger import logger
from utils import (
    get_last,
    get_config,
    secrets,
    build_file_path,
    get_endpoints,
)

from notifiers import get_notifier

import ssl

ssl._create_default_https_context = ssl._create_unverified_context


def _write_data(data, endpoint):

    output_path = build_file_path(endpoint)

    data["data_last_refreshed"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data.to_csv(output_path, index=False)
    logger.info("WRITTING DATA FOR {}", endpoint["python_file"])


def _test_data(data, tests, endpoint):

    results = [v(data) for k, v in tests.items()]

    if not all(results):
        logger.info("TESTS FAILED FOR {}", endpoint["python_file"])
        for k, v in tests.items():
            if not v(data):

                logger.error(
                    "TEST FAILED FOR ENDPOINT {}: {}", endpoint["python_file"], k
                )

        return False
    else:
        logger.info("TESTS PASSED FOR {}", endpoint["python_file"])
        return True


@logger.catch
def main(endpoint):

    logger.info("STARTING: {}", endpoint["python_file"])

    runner = importlib.import_module("endpoints.{}".format(endpoint["python_file"]))

    data = runner.now(get_config(), force=True)

    if _test_data(data, runner.TESTS, endpoint):

        _write_data(data, endpoint)


if __name__ == "__main__":

    for endpoint in get_endpoints():

        if endpoint.get("skip"):
            continue

        main(endpoint)
