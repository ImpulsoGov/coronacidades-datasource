import os
from time import sleep
import yaml

from main import main
from logger import log


def handler(endpoint):

    try:
        main(endpoint)
    except Exception as e:
        log(
            {"origin": "Datasource Scheduler", "error_type": "Generic", "error": e},
            status="fail",
        )
        print(e)


def start():

    endpoints = yaml.load(open("endpoints.yaml", "r"))

    while True:

        for endpoint in endpoints:

            handler(endpoint)

        sleep(float(os.getenv("REFRESH_RATE_MINUTES")) * 60)


if __name__ == "__main__":

    start()
