import pandas as pd
from utils import secrets
from endpoints.helpers import allow_local
import endpoints.GoogleDocsLib as gd
import os
import numpy as np

configs_path = os.path.join(os.path.dirname(__file__), "aux")


@allow_local
def now(config):
    file_id = secrets(["inloco", "states", "id"])
    df = gd.downloadGoogleFileDF(file_id, "token.pickle")
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
