import pandas as pd
from utils import get_googledrive_df, configs_path
from endpoints.helpers import allow_local
import os
import numpy as np


@allow_local
def now(config):

    df = get_googledrive_df(os.getenv("INLOCO_STATES_ID"))
    states_table = pd.read_csv(os.path.join(configs_path, "states_table.csv"))
    states_table = states_table.sort_values(by=["state_name"])
    states_num_ids = states_table["state_num_id"].values
    csid = np.vectorize(
        lambda state_name: states_num_ids[
            np.searchsorted(states_table["state_name"].values, state_name)
        ]
    )
    df["state_num_id"] = csid(df["state_name"].values)
    return df


TESTS = {
    "data is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
}
