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


# TODO: alert by regions
# def _get_population_alert(group, mean_pop):
#     # group["population"].cumsum().searchsorted(mean_pop[group.index[1]]).reset()
#     # .query(f"population < {mean_pop}")
#     # .drop_duplicated(["state_num_id"], keep="first"))


# def get_weighted_level(regions):

#     mean_pop = regions.groubpy("state_num_id")['population'].sum()/2

#     max_regions_alert = regions.groubpy("state_num_id")["overall_alert"].apply(lambda x: ceil(np.quantile(x, 0.5)))

#     max_population_alert = (regions.sort_values(["overall_alert", "state_num_id"])
#     .groubpy(["overall_alert", "state_num_id"]).apply(lambda group: _get_population_alert(group, mean_pop), axis=1)

#     return pd.concat([max_regions_alert, max_population_alert])["overall_alert"].max(level=0)


@allow_local
def now(config):

    # Get last cases data
    cases = (
        get_states_cases.now(config, "br")
        .dropna(subset=["active_cases"])
        .assign(last_updated=lambda df: pd.to_datetime(df["last_updated"]))
    )

    cases = cases.loc[cases.groupby("state_num_id")["last_updated"].idxmax()]
    # .drop(
    #     config["br"]["cases"]["drop"] + ["state_num_id", "health_region_id"], 1
    # )

    # Merge resource data
    df = (
        get_health.now(config, "br")
        .groupby(
            [
                "country_iso",
                "country_name",
                "state_num_id",
                "last_updated_number_beds",
                "author_number_beds",
                "last_updated_number_icu_beds",
                "author_number_icu_beds",
            ]
        )
        .agg({"population": sum, "number_beds": sum, "number_icu_beds": sum})
        .reset_index()
        .merge(cases, on="state_num_id", how="left")
    )

    df = (
        df.sort_values("state_num_id")
        .assign(confirmed_cases=lambda x: x["confirmed_cases"].fillna(0))
        .assign(deaths=lambda x: x["deaths"].fillna(0))
        .reset_index()
        .set_index("state_num_id")
    )

    # TODO: get_cases => get_states_cases / mudar indicadores de situacao + add trust (notification_rate)!
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

    df = get_capacity_indicators(
        df,
        place_id="state_num_id",
        config=config,
        rules=config["br"]["farolcovid"]["rules"],
        classify="capacity_classification",
    )

    df = get_trust_indicators(
        df,
        data=get_states_cases.now(config),
        place_id="state_num_id",
        rules=config["br"]["farolcovid"]["rules"],
        classify="trust_classification",
    )

    cols = [col for col in df.columns if "classification" in col]

    # TODO: Overall alert - max of cumulative regions in level
    # regions = get_weighted_level(get_health_region_farolcovid_main.now(config))

    df["overall_alert"] = df.apply(
        lambda row: get_overall_alert(row[cols]), axis=1
    )  # .replace(config["br"]["farolcovid"]["categories"])

    return df.reset_index()


TESTS = {
    "doesnt have 27 states": lambda df: len(df["state_num_id"].unique()) == 27,
    "df is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    "overall alert < 3": lambda df: all(
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
