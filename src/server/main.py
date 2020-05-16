from flask import Flask

app = Flask(__name__)

import json
import pandas as pd
import os
import yaml

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
                "\n".join(os.listdir(os.getenv("OUTPUT_DIR")))
            )


if __name__ == "__main__":

    # Only for debugging while developing
    app.run(host="0.0.0.0", debug=True, port=7000)
