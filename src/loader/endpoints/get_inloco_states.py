import pandas as pd
from utils import get_googledrive_df, configs_path, download_from_drive
from endpoints.helpers import allow_local
import os
import numpy as np


@allow_local
def now(config):

    df = get_googledrive_df(os.getenv("INLOCO_STATES_ID"))

    states_table = (
        download_from_drive(config["br"]["drive_paths"]["br_id_state_region_city"])[
            ["state_id", "state_name", "state_num_id"]
        ]
        .drop_duplicates()
        .sort_values(by=["state_name"])
    )

    return df.merge(states_table, on="state_name")


TESTS = {
    "data is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    "dataframe has null data": lambda df: all(df.isnull().any() == False),
    "dataframe doesnt have some states": lambda df: len(df["state_num_id"].unique())
    == 27,
    "isolation index has negative data": lambda df: len(df.query("isolated < 0")) == 0,
    "isolation index is more than 100%": lambda df: len(df.query("isolated > 1")) == 0,
}
