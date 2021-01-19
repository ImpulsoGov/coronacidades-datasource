import pandas as pd
import numpy as np
from endpoints.helpers import allow_local
from endpoints import get_states_cases
from endpoints.get_cities_rt import get_rt


@allow_local
def now(config=None):

    # Import cases
    df = get_states_cases.now(config, "br")
    df["last_updated"] = pd.to_datetime(df["last_updated"])

    return get_rt(df, "state_num_id", config)


TESTS = {
    "data is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    # "dataframe has null data": lambda df: all(df.isnull().any() == False),
    # "not all 27 states with updated rt": lambda df: len(
    #     df.drop_duplicates("state_num_id", keep="last")
    # )
    # == 27,
    "rt most likely outside confidence interval": lambda df: len(
        df[
            (df["Rt_most_likely"] <= df["Rt_high_95"])
            & (df["Rt_most_likely"] >= df["Rt_low_95"])
        ]
    )
    == len(df),
    "state has rt with less than 14 days": lambda df: all(
        df.groupby("state_num_id")["last_updated"].count() > 14
    )
    == True,
}
