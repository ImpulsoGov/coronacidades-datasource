import pandas as pd
import numpy as np
import datetime as dt
import yaml

from endpoints import (
    get_simulacovid_main,
    get_city_cases,
    get_states_rt,
    get_inloco_states,
)
from endpoints.get_cities_farolcovid_main import (
    get_indicators_subnotification,
    get_indicators_rt,
    get_indicators_inloco,
    get_indicators_capacity,
    get_overall_alert,
)
from endpoints.helpers import allow_local


@allow_local
def now(config):

    df = (
        get_simulacovid_main.now(config)
        .sort_values("state_num_id")
        .groupby(["state_num_id", "state_id", "state_name"])
        .agg(config["br"]["farolcovid"]["simulacovid"]["state_agg"])
        .rename({"state_notification_rate": "notification_rate"}, axis=1)
        .assign(confirmed_cases=lambda x: x["confirmed_cases"].fillna(0))
        .assign(deaths=lambda x: x["deaths"].fillna(0))
        .reset_index()
        .set_index("state_num_id")
    )

    # Calcula indicadores, classificações e crescimento
    df = get_indicators_subnotification(
        df,
        data=get_city_cases.now(config),
        place_id="state_num_id",
        rules=config["br"]["farolcovid"]["rules"],
        classify="subnotification_classification",
    )

    df = get_indicators_rt(
        df,
        data=get_states_rt.now(config),
        place_id="state_num_id",
        rules=config["br"]["farolcovid"]["rules"],
        classify="rt_classification",
        growth="rt_growth",
    )

    df = get_indicators_inloco(
        df,
        data=get_inloco_states.now(config),
        place_id="state_num_id",
        rules=config["br"]["farolcovid"]["rules"],
        growth="inloco_growth",
    )

    df = get_indicators_capacity(
        df,
        place_id="state_num_id",
        config=config,
        rules=config["br"]["farolcovid"]["rules"],
        classify="dday_classification",
    )

    df["overall_alert"] = df.apply(
        lambda x: get_overall_alert(x, config["br"]["farolcovid"]["alerts"]), axis=1
    ).replace("medio2", "medio")

    return df.reset_index()


TESTS = {
    "df is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    "the total is not 27 states": lambda df: len(df["state_num_id"].unique()) == 27,
    # "dataframe has null data": lambda df: all(df.isnull().any() == False),
    "dday worst greater than best": lambda df: len(
        df[df["dday_beds_worst"] > df["dday_beds_best"]]
    )
    == 0,
    "state with rt classified doesnt have rt growth": lambda df: len(
        df[(~df["rt_classification"].isnull()) & (df["rt_growth"].isnull())]
    )
    == 0,
    "state with all classifications got null alert": lambda df: all(
        df.dropna(
            subset=[
                "rt_classification",
                "rt_growth",
                "dday_classification",
                "subnotification_classification",
            ],
            how="any",
        )["overall_alert"].isnull()
        == False
    ),
}
