import yaml
from main import main
from logger import log
import time
from timeloop import Timeloop
from datetime import timedelta


def handler(endpoint):

    try:
        print("\n===> STARTING: {}\n\n".format(endpoint["python_file"]))
        main(endpoint)
        print("\n===> UPDATED NOW: {}\n\n".format(endpoint["python_file"]))

    except Exception as e:
        # log({"origin": "Datasource Scheduler",
        #     "error_type": "Generic",
        #     "error": e},
        #     status="fail")
        print(e)


endpoints = yaml.load(open("endpoints.yaml", "r"), Loader=yaml.FullLoader)

tl = Timeloop()
for e in endpoints:
    tl._add_job(
        func=handler,
        interval=timedelta(minutes=e["update_frequency_minutes"]),
        endpoint=e,
    )

if __name__ == "__main__":
    tl.start(block=True)
