import pandas as pd
import numpy as np
import datetime as dt
import yaml
from math import ceil

from endpoints import (
    get_states_cases,
    get_states_rt,
    get_health,
    get_health_region_farolcovid_main,
)
from endpoints.get_cities_farolcovid_main import (
    get_situation_indicators,
    get_control_indicators,
    get_capacity_indicators,
    get_trust_indicators,
    get_overall_alert,
)
from endpoints.helpers import allow_local


def _get_weighted_level(df_regions):

    # Get max alert of at least half regions
    max_regions_alert = (
        df_regions.dropna(subset=["overall_alert"])
        .groupby("state_num_id")["overall_alert"]
        .apply(lambda x: ceil(np.quantile(x, 0.5)))
    )

    # Get state cumulative population by alert
    max_pop_alert = (
        df_regions.groupby(["state_num_id", "overall_alert"])["population"]
        .sum()
        .groupby(level=0)
        .cumsum()
        .reset_index()
    )

    # Compare to state population mean and get max alert of at least half population
    max_pop_alert = (
        max_pop_alert.merge(
            max_pop_alert.groupby("state_num_id")["population"].apply(
                lambda x: max(x) / 2
            ),
            on="state_num_id",
            suffixes=("", "_mean"),
        )
        .query("population >= population_mean")
        .drop_duplicates(subset=["state_num_id"], keep="first")
        .set_index("state_num_id")["overall_alert"]
    )

    # Get max overall alert between population and regions POV
    return pd.concat([max_pop_alert, max_regions_alert], axis=1).max(axis=1)


@allow_local
def now(config):

    # Get resource data
    df = (
        get_health.now(config, "br")
        .groupby(
            [
                "country_iso",
                "country_name",
                "state_num_id",
                "state_id",
                "state_name",
                "last_updated_number_beds",
                "author_number_beds",
                "last_updated_number_icu_beds",
                "author_number_icu_beds",
            ]
        )
        .agg({"population": sum, "number_beds": sum, "number_icu_beds": sum})
        .reset_index()
        .sort_values("state_num_id")
        .set_index("state_num_id")
    )

    df = get_situation_indicators(
        df,
        data=get_states_cases.now(config),
        place_id="state_num_id",
        rules=config["br"]["farolcovid"]["rules"],
        classify="situation_classification",
    )

    df = get_control_indicators(
        df,
        data=get_states_rt.now(config),
        place_id="state_num_id",
        rules=config["br"]["farolcovid"]["rules"],
        classify="control_classification",
    )

    df = get_trust_indicators(
        df,
        data=get_states_cases.now(config),
        place_id="state_num_id",
        rules=config["br"]["farolcovid"]["rules"],
        classify="trust_classification",
    )

    df = get_capacity_indicators(
        df,
        place_id="state_num_id",
        config=config,
        rules=config["br"]["farolcovid"]["rules"],
        classify="capacity_classification",
    )

    cols = [col for col in df.columns if "classification" in col]

    # TODO: Overall alert - max of cumulative regions in level
    df["overall_alert"] = _get_weighted_level(
        get_health_region_farolcovid_main.now(config)
    )

    # df["overall_alert"] = df.apply(
    #     lambda row: get_overall_alert(row[cols]), axis=1
    # ) # .replace(config["br"]["farolcovid"]["categories"])

    return df.reset_index()


TESTS = {
    "doesnt have 27 states": lambda df: len(df["state_num_id"].unique()) == 27,
    "df is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    "overall alert > 3": lambda df: all(
        df[~df["overall_alert"].isnull()]["overall_alert"] <= 3
    ),
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
    # "state without classification got an alert": lambda df: all(
    #     df[
    #         df[
    #             [
    #                 "capacity_classification",
    #                 "control_classification",
    #                 "situation_classification",
    #                 "trust_classification",
    #             ]
    #         ]
    #         .isnull()
    #         .any(axis=1)
    #     ]["overall_alert"].isnull()
    #     == True
    # ),
}
