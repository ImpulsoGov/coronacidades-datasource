from flask import Flask, render_template, request


app = Flask(__name__)

import json
import pandas as pd
import os
import yaml


def _load_data(entry, query_parameters):
    path = "/".join([os.getenv("OUTPUT_DIR"), entry.replace("/", "-")]) + ".csv"
    data = pd.read_csv(path)
    if query_parameters.get('state_id'):
        data = data[data["state_id"]==query_parameters.get('state_id')]
    if query_parameters.get('city_id'):
        data = data[data["city_id"]==query_parameters.get('city_id')]
    if query_parameters.get('city_name'):
        data = data[data["city_name"]==query_parameters.get('city_name')]
    return data.to_csv(index=False)


@app.route('/<path:entry>', methods=['GET'])
def index(entry):
    if entry is None:
        return "This is an API"  # for example
    else:
        try:
            return _load_data(entry, request.args)

        except FileNotFoundError:
            endpoints = [
                d.split(".")[0].replace("-", "/")
                for d in os.listdir(os.getenv("OUTPUT_DIR"))
                if "inloco" not in d
            ]
            return render_template("not-found.html", endpoints=endpoints, query_parameters=request.args)


if __name__ == "__main__":

    # Only for debugging while developing
    app.run(host="0.0.0.0", debug=True, port=7000)
