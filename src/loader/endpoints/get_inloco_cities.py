import pandas as pd
from utils import secrets


def now(config):
    return pd.read_csv(secrets(["inloco", "cities", "url"]))


TESTS = {
    "data is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
}