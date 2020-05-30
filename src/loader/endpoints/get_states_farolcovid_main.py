import pandas as pd
import numpy as np
import datetime as dt
import yaml

from endpoints import (
    get_simulacovid_main,
    get_cases,
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

    config["farolcovid"] = yaml.load(
        open("endpoints/aux/farol_config.yaml"), Loader=yaml.FullLoader
    )

    df = (
        pd.read_csv(
            "http://datasource.coronacidades.org:7000/br/cities/simulacovid/main"
        )[config["farolcovid"]["simulacovid"]["columns"]]
        .sort_values("state_id")
        .groupby(["state_id", "state_name"])
        .agg(config["farolcovid"]["simulacovid"]["state_agg"])
        .rename({"state_notification_rate": "notification_rate"}, axis=1)
        .assign(confirmed_cases=lambda x: x["confirmed_cases"].fillna(0))
        .assign(deaths=lambda x: x["deaths"].fillna(0))
        .assign(
            number_beds=lambda x: config["farolcovid"]["resources"][
                "available_proportion"
            ]
            * x["number_beds"]
            / config["br"]["health"]["initial_proportion"]
        )
        .assign(
            number_ventilators=lambda x: config["farolcovid"]["resources"][
                "available_proportion"
            ]
            * x["number_ventilators"]
            / config["br"]["health"]["initial_proportion"]
        )
        .reset_index()
        .set_index("state_id")
    )

    # Calcula indicadores, classificações e crescimento
    df = get_indicators_subnotification(
        df,
        data=get_cases.now(config),
        place_id="state",
        rules=config["farolcovid"]["rules"],
        classify="subnotification_classification",
    )

    df = get_indicators_rt(
        df,
        data=get_states_rt.now(config),
        place_id="state",
        rules=config["farolcovid"]["rules"],
        classify="rt_classification",
        growth="rt_growth",
    )

    df = get_indicators_inloco(
        df,
        data=get_inloco_states.now(config),
        place_id="state_id",
        rules=config["farolcovid"]["rules"],
        growth="inloco_growth",
    )

    df = get_indicators_capacity(
        df, config, rules=config["farolcovid"]["rules"], classify="dday_classification"
    )

    df["overall_alert"] = df.apply(
        lambda x: get_overall_alert(x, config["farolcovid"]["alerts"]), axis=1
    ).replace("medio2", "medio")

    return df.reset_index()


TESTS = {
    "more than 27 states": lambda df: len(df["state_id"].unique()) <= 27,
    "df is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    "state doesnt have both rt classified and growth": lambda df: df[
        "rt_classification"
    ].count()
    == df["rt_growth"].count(),
    "dday worst greater than best": lambda df: len(
        df[df["dday_beds_worst"] > df["dday_beds_best"]]
    )
    == 0,
    "state with all classifications got null alert": lambda df: all(
        df[df["overall_alert"].isnull()][
            [
                "rt_classification",
                "rt_growth",
                "dday_classification",
                "subnotification_classification",
            ]
        ]
        .isnull()
        .apply(lambda x: any(x), axis=1)
        == True
    ),
    # "state without deaths has mortality ratio": lambda df: len(
    #     df[(df["deaths"] == 0) & (~df["last_mortality_ratio"].isnull())]
    # )
    # == 0,
}
