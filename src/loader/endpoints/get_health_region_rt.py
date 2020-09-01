import pandas as pd
import numpy as np
from endpoints.helpers import allow_local
from endpoints import get_health_region_cases, get_cities_rt


@allow_local
def now(config=None):
    return get_cities_rt.get_rt(
        get_health_region_cases.now(), place_id="health_region_id"
    )


# TODO: review tests
TESTS = {
    "data is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    "dataframe has null data": lambda df: all(
        df[["Rt_most_likely", "Rt_high_95", "Rt_low_95"]].isnull().any() == False
    ),
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
