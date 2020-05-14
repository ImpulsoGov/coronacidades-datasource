from flask import Flask

app = Flask(__name__)

import json
import pandas as pd
import os


def get_endpoints(url=os.getenv("ENDPOINTS_URL")):
    return yaml.load(requests.get(url).text, Loader=yaml.FullLoader)


def _load_data(entry):

    path = "/".join([os.getenv("OUTPUT_DIR"), entry.replace("/", "-")]) + ".csv"

    data = pd.read_csv(path)

    return data.to_csv(index=False)


@app.route("/<path:entry>")
def index(entry):

    if entry is None:
        return "This is an API"  # for example
    else:

        try:
            return _load_data(entry)
        except FileNotFoundError:
            return (
                "This endpoint does not exist\n"
                "Please try one of the following:\n "
                "\n".join([e["endpoint"] for e in get_endpoints()])
            )


if __name__ == "__main__":

    # Only for debugging while developing
    app.run(host="0.0.0.0", debug=True, port=80)
