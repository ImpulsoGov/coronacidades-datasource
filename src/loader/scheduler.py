import os
from time import sleep

from main import main


def start():

    while True:
        try:
            main()
        except Exception as e:
            print(e)

        sleep(float(os.getenv("REFRESH_RATE_MINUTES")) * 60)


if __name__ == "__main__":

    start()
