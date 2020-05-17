import pandas as pd

from utils import secrets


def now(config):
    """This method is going to be called by main.py and it should return the output
    DataFrame.

    Parameters
    ----------
    config : dict
    """

    return pd.read_csv(secrets(["inloco", "states", "url"]))


# Output dataframe tests to check data integrity. This is also going to be called
# by main.py
TESTS = {
    "data is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
}
