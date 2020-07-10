import pandas as pd
import datetime
import numpy as np
from urllib.request import Request, urlopen
import json
import time

import gzip
import io
from urllib.request import Request, urlopen

from endpoints.helpers import allow_local
from utils import download_from_drive


@allow_local
def now(config, country="br"):

    return download_from_drive(
        config[country]["drive_paths"]["br_id_state_region_city"]
    )


TESTS = {
    "not 5570 cities": lambda df: len(df["city_id"].unique()) == 5570,
    "not 27 states": lambda df: len(df["state_id"].unique()) == 27,
    "not all states have unique id": lambda df: len(
        df[["state_num_id", "state_id", "state_name"]].drop_duplicates()
    )
    == 27,
    "not all cities have unique id": lambda df: len(
        df[["city_id", "city_name"]].drop_duplicates()
    )
    == 5570,
    "df is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
}
