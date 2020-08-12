from utils import get_cases_series
from endpoints import get_city_cases, get_cities_rt
import pandas as pd
import numpy as np
from endpoints.helpers import allow_local
from endpoints import get_cases, get_cities_rt


@allow_local
def now(config):

    # Import cases
    df = get_city_cases.now(config, "br")
    df["last_updated"] = pd.to_datetime(df["last_updated"])

    # Filter more than 14 days & run
    df = get_cities_rt.sequential_run(
        get_cases_series(
            df, "health_region_id", config["br"]["rt_parameters"]["min_days"]
        ),
        config,
        place_type="health_region_id",
    )


# TODO: review tests
TESTS = {
    "data is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    "dataframe has null data": lambda df: all(df.isnull().any() == False),
    "rt most likely outside confidence interval": lambda df: len(
        df[
            (df["Rt_most_likely"] >= df["Rt_high_95"])
            & (df["Rt_most_likely"] <= df["Rt_high_95"])
        ]
    )
    == 0,
    # "region has rt with less than 14 days": lambda df: all(
    #     df.groupby("health_region_id")["last_updated"].count() > 14
    # )
    # == True,
}
