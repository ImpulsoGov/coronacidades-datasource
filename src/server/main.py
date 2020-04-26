from flask import Flask
app = Flask(__name__)

import os
import json
import pandas as pd

def _get_raw_data():

    output_path= '/'.join([os.getenv('OUTPUT_DIR'),
                           os.getenv('RAW_NAME')]) + '.csv'

    return pd.read_csv(output_path)


@app.route("/v1/raw/json")
def get_raw_json():
    
    data = _get_raw_data()

    return data.to_json(orient='records')


@app.route("/v1/raw/csv")
def get_raw_csv():

    data = _get_raw_data()

    return data.to_csv(index=False)

if __name__ == "__main__":
    # Only for debugging while developing
    app.run(host='0.0.0.0', debug=True, port=7000)
