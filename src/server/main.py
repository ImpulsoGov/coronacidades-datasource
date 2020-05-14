from flask import Flask
app = Flask(__name__)

import json
import pandas as pd
from utils import get_endpoints
import os

# O que funciona:

# @app.route("/br/cities/embaixadores")
# def get_data(): 
#     data = _load_data("br/cities/embaixadores")
#     return data.to_csv(index=False)

# O que não funciona: -- TypeError: 'str' object is not callable
entrypoints = get_endpoints()

def _load_data(entry):

    path = '/'.join([os.getenv('OUTPUT_DIR'), entry.replace("/", "-")]) + '.csv'

    data = pd.read_csv(path)

    return data.to_csv(index=False)

def entrypoint(entry):
    return _load_data(entry)

entrypoints = get_endpoints()

for entry in entrypoints:
    app.add_url_rule(
        "/" + entry["endpoint"],
        "entrypoint({})".format(entry["endpoint"]),
        entrypoint(entry["endpoint"])
    )

# Para checar as opções na API
print(app.url_map)

if __name__ == "__main__":

    # Only for debugging while developing
    app.run(host='0.0.0.0', debug=True, port=80)
