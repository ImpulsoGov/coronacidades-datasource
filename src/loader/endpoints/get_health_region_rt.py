import pandas as pd
import numpy as np
from endpoints.helpers import allow_local
from endpoints import get_health_region_cases, get_cities_cases
from endpoints.get_cities_rt import get_rt


@allow_local
def now(config=None):

    # Import cases
    df = get_health_region_cases.now(config, "br")
    df["last_updated"] = pd.to_datetime(df["last_updated"])

    return get_rt(df, "health_region_id", config)


TESTS = {
    "data is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    # "dataframe has null data": lambda df: all(df.isnull().any() == False),
    # "not all 27 states with updated rt": lambda df: len(
    #     df.drop_duplicates("health_region_id", keep="last")
    # )
    # == 27,
    "rt most likely outside confidence interval": lambda df: len(
        df[
            (df["Rt_most_likely"] <= df["Rt_high_95"])
            & (df["Rt_most_likely"] >= df["Rt_low_95"])
        ]
    )
    == len(df),
    # "region has rt with less than 14 days": lambda df: all(
    #     df.groupby("health_region_id")["last_updated"].count() > 14
    # )
    # == True,
}
