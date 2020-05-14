import pandas as pd
import requests
from copy import deepcopy
import os
import yaml
from datetime import datetime
import numpy as np
import importlib
from logger import log
from utils import get_last, get_config

import ssl

ssl._create_default_https_context = ssl._create_unverified_context


def _write_data(data, endpoint):

    output_path = (
        "/".join([os.getenv("OUTPUT_DIR"), endpoint["endpoint"].replace("/", "-")])
        + ".csv"
    )

    data["data_last_refreshed"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data.to_csv(output_path, index=False)


def _test_data(data, tests):

    if not all(tests.values()):

        for k, v in tests.items():
            if not v(data):
                log(
                    {
                        "origin": "Raw Data", 
                        "error_type": "Data Integrity",
                        "error": k},
                    status="fail",
                )
                print("Error in: ", k)

        return False
    else:
        # log(dict(), status='okay')
        return True


def main(endpoint):

    runner = importlib.import_module("endpoints.{}".format(endpoint['python_file']))

    data = runner.now(get_config())

    if _test_data(data, runner.TESTS):

        _write_data(data, endpoint)


if __name__ == "__main__":

    print("\n==> STARTING: Getting endpoints configuration from endpoints.yaml...")
    config_endpoints = yaml.load(open("endpoints.yaml", "r"), Loader=yaml.FullLoader)

    for endpoint in config_endpoints:

        print("\n==> LOADING: {}\n".format(endpoint['endpoint']))
        main(endpoint)
        print("\n\n==> NEXT!")
    
    print("=> DONE!")
