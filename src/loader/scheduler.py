import yaml
from main import main
import time
from timeloop import Timeloop
from datetime import timedelta

from logger import logger


@logger.catch
def handler(endpoint):

    try:
        main(endpoint)

    except Exception as e:

        logger.error("SCHEDULER ENDPOINT FAILED: {}", endpoint["python_file"])


if __name__ == "__main__":

    endpoints = yaml.load(open("endpoints.yaml", "r"), Loader=yaml.FullLoader)

    tl = Timeloop()
    for e in endpoints:
        tl._add_job(
            func=handler,
            interval=timedelta(minutes=e["update_frequency_minutes"]),
            endpoint=e,
        )

    for job in tl.jobs:
        job.execute(*job.args, **job.kwargs)

    tl.start(block=True)
