import pandas as pd
import numpy as np
import datetime as dt
import yaml

from endpoints import get_cases, get_states_rt, get_health
from endpoints.get_cities_farolcovid_main import (
    get_situation_indicators,
    get_control_indicators,
    get_capacity_indicators,
    get_trust_indicators,
    get_overall_alert,
)
from endpoints.helpers import allow_local


@allow_local
def now(config):

    # Get last cases data
    cases = (
        get_cases.now(config, "br")
        .dropna(subset=["active_cases"])
        .assign(last_updated=lambda df: pd.to_datetime(df["last_updated"]))
    )
    cases = cases.loc[cases.groupby("city_id")["last_updated"].idxmax()].drop(
        config["br"]["cases"]["drop"] + ["state_num_id", "health_region_id"], 1
    )

    # Merge resource data
    df = get_health.now(config, "br")[
        config["br"]["simulacovid"]["columns"]["cnes"]
    ].merge(cases, on="city_id", how="left")

    df = (
        df.sort_values("state_num_id")
        .groupby(["state_num_id", "state_id", "state_name"])
        .agg(config["br"]["farolcovid"]["simulacovid"]["state_agg"])
        .assign(confirmed_cases=lambda x: x["confirmed_cases"].fillna(0))
        .assign(deaths=lambda x: x["deaths"].fillna(0))
        .reset_index()
        .set_index("state_num_id")
    )

    # TODO: get_cases => get_states_cases / mudar indicadores de situacao + add trust (notification_rate)!
    df = get_situation_indicators(
        df,
        data=get_cases.now(config),
        place_id="state_num_id",
        rules=config["br"]["farolcovid"]["rules"],
        classify="situation_classification",
        growth=None,  # -> "situation_growth" depois do update na tabela de casos
    )

    df = get_control_indicators(
        df,
        data=get_states_rt.now(config),
        place_id="state_num_id",
        rules=config["br"]["farolcovid"]["rules"],
        classify="control_classification",
        growth=None,  # "control_growth",
    )

    df = get_capacity_indicators(
        df,
        place_id="state_num_id",
        config=config,
        rules=config["br"]["farolcovid"]["rules"],
        classify="capacity_classification",
    )

    df = get_trust_indicators(
        df,
        data=get_cases.now(config),
        place_id="state_num_id",
        rules=config["br"]["farolcovid"]["rules"],
        classify="trust_classification",
        growth=None,  # "trust_growth",
    )

    cols = [col for col in df.columns if "classification" in col]
    df["overall_alert"] = df.apply(
        lambda row: get_overall_alert(row[cols]), axis=1
    )  # .replace(config["br"]["farolcovid"]["categories"])

    return df.reset_index()


TESTS = {
    "doesnt have 27 states": lambda df: len(df["state_num_id"].unique()) == 27,
    "df is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    # "dataframe has null data": lambda df: all(df.isnull().any() == False),
    # "state doesnt have both rt classified and growth": lambda df: df[
    #     "control_classification"
    # ].count()
    # == df["control_growth"].count(),
    "dday worst greater than best": lambda df: len(
        df[df["dday_icu_beds_best"] < df["dday_icu_beds_worst"]]
    )
    == 0,
    "rt 10 days maximum and minimum values": lambda df: all(
        df[
            ~(
                (df["rt_low_95"] < df["rt_most_likely"])
                & (df["rt_most_likely"] < df["rt_high_95"])
            )
        ]["rt_most_likely"].isnull()
    ),
    "state with all classifications got null alert": lambda df: all(
        df[df["overall_alert"].isnull()][
            [
                "control_classification",
                "situation_classification",
                "capacity_classification",
                "trust_classification",
            ]
        ]
        .isnull()
        .apply(lambda x: any(x), axis=1)
        == True
    ),
    "state without classification got an alert": lambda df: all(
        df[
            df[
                [
                    "capacity_classification",
                    "control_classification",
                    "situation_classification",
                    "trust_classification",
                ]
            ]
            .isnull()
            .any(axis=1)
        ]["overall_alert"].isnull()
        == True
    ),
}
