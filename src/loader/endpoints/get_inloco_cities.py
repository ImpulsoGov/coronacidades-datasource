import pandas as pd
from utils import secrets, get_googledrive_df
from logger import logger
from endpoints.helpers import allow_local
import os

@allow_local
def now(config):

    return get_googledrive_df(
            secrets(["inloco", "cities", "id"]), 
            "token.pickle")


TESTS = {
    "data is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
}
