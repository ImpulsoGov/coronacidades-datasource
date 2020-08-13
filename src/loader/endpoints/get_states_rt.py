import pandas as pd
import numpy as np
from endpoints.helpers import allow_local
from endpoints import get_city_cases, get_cities_rt


@allow_local
def now(config=None):
    # TODO: mudar para get_[cities/region/states]_cases quando tiver as tabelas
    return get_cities_rt.get_rt(get_city_cases.now(), place_id="state_num_id")


# TODO: review tests
TESTS = {
    "data is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    "dataframe has null data": lambda df: all(df.isnull().any() == False),
    # "not all 27 states with updated rt": lambda df: len(
    #     df.drop_duplicates("state_num_id", keep="last")
    # )
    # == 27,
    "rt most likely outside confidence interval": lambda df: len(
        df[
            (df["Rt_most_likely"] >= df["Rt_high_95"])
            & (df["Rt_most_likely"] <= df["Rt_high_95"])
        ]
    )
    == 0,
    # "state has rt with less than 14 days": lambda df: all(
    #     df.groupby("state_num_id")["last_updated"].count() > 14
    # )
    # == True,
}
