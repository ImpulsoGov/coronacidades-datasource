import os
from time import sleep
from loguru import logger

from main import main

logger.add("scheduler.log", backtrace=True, diagnose=True)

def start():

    while True:
        main()
        sleep(float(os.getenv('REFRESH_RATE_MINUTES')) * 60)


if __name__ == "__main__":

    start()