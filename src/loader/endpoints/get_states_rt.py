from utils import get_cases_series
from endpoints import get_cases, get_cities_rt
import pandas as pd
import numpy as np

from endpoints.helpers import allow_local


@allow_local
def now(config):

    # Import cases
    df = get_cases.now(config, "br")
    df["last_updated"] = pd.to_datetime(df["last_updated"])

    # Filter more than 14 days
    df = get_cases_series(df, "state", config["br"]["rt_parameters"]["min_days"])

    # Run in parallel
    return get_cities_rt.sequential_run(df, config, place_type="state")


TESTS = {
    "data is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    "dataframe has null data": lambda df: all(df.isnull().any() == False),
    "not all 27 states with updated rt": lambda df: len(
        df.drop_duplicates("state", keep="last")
    )
    == 27,
    "rt most likely outside confidence interval": lambda df: len(
        df[
            (df["Rt_most_likely"] >= df["Rt_high_95"])
            & (df["Rt_most_likely"] <= df["Rt_high_95"])
        ]
    )
    == 0,
    "state has rt with less than 14 days": lambda df: all(
        df.groupby("state")["last_updated"].count() > 14
    )
    == True,
}
