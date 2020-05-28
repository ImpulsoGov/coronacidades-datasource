import pandas as pd
from utils import secrets, get_googledrive_df
from endpoints.helpers import allow_local


@allow_local
def now(config):

    return get_googledrive_df(
            secrets(["inloco", "states", "id"]),
            "token.pickle")


TESTS = {
    "data is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
}
