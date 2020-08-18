import pandas as pd
import numpy as np
import datetime as dt
import yaml

from endpoints import (
    get_cities_cases,
    get_cities_rt,
    get_health_region_rt,
    get_health,
    get_health_region_farolcovid_main,
)

from endpoints.get_health_region_farolcovid_main import (
    get_situation_indicators,
    get_control_indicators,
    get_capacity_indicators,
    get_trust_indicators,
    get_overall_alert,
)

from endpoints.helpers import allow_local


@allow_local
def now(config):

    # Get resource data
    df = (
        get_health.now(config, "br")[
            [
                "country_iso",
                "country_name",
                "state_num_id",
                "state_id",
                "state_name",
                "health_region_id",
                "health_region_name",
                "city_name",
                "city_id",
                "last_updated_number_beds",
                "author_number_beds",
                "last_updated_number_icu_beds",
                "author_number_icu_beds",
            ]
        ]
        .sort_values("city_id")
        .set_index("city_id")
    )

    df = get_situation_indicators(
        df,
        data=get_cities_cases.now(config),
        place_id="city_id",
        rules=config["br"]["farolcovid"]["rules"],
        classify="situation_classification",
    )

    df = get_control_indicators(
        df,
        data=get_cities_rt.now(config),
        place_id="city_id",
        rules=config["br"]["farolcovid"]["rules"],
        classify="control_classification",
        config=config,
        region_data=get_health_region_rt.now(config),
    )

    df = get_trust_indicators(
        df,
        data=get_cities_cases.now(config),
        place_id="city_id",
        rules=config["br"]["farolcovid"]["rules"],
        classify="trust_classification",
    )

    df = get_capacity_indicators(
        df,
        place_id="city_id",
        config=config,
        rules=config["br"]["farolcovid"]["rules"],
        classify="capacity_classification",
        data=get_health_region_farolcovid_main.now(config),
    )

    cols = [col for col in df.columns if "classification" in col]
    df["overall_alert"] = df.apply(
        lambda row: get_overall_alert(row[cols]), axis=1
    )  # .replace(config["br"]["farolcovid"]["categories"])

    return df.reset_index()


TESTS = {
    "doesnt have 5570 cities": lambda df: len(df["city_id"].unique()) == 5570,
    "doesnt have 27 states": lambda df: len(df["state_num_id"].unique()) == 27,
    "df is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    "overall alert > 3": lambda df: all(
        df[~df["overall_alert"].isnull()]["overall_alert"] <= 3
    ),
    # "doesnt have both rt classified and growth": lambda df: df[
    #     "control_classification"
    # ].count()
    # == df["rt_most_likely_growth"].count(),
    "rt 10 days maximum and minimum values": lambda df: all(
        df[
            ~(
                (df["rt_low_95"] < df["rt_most_likely"])
                & (df["rt_most_likely"] < df["rt_high_95"])
            )
        ]["rt_most_likely"].isnull()
    ),
    "city with all classifications got null alert": lambda df: all(
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
    "city without classification got an alert": lambda df: all(
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
