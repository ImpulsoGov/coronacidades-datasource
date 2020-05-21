import pandas as pd
import numpy as np
import datetime as dt

from endpoints import get_cities_rt, get_cases, get_inloco_cities, get_simulacovid_main
from endpoints.helpers import allow_local


def _get_rt_indicators(df, config):

    rt = get_cities_rt.now(config)
    rt["last_updated"] = pd.to_datetime(rt["last_updated"])

    # result = pd.DataFrame()
    for k, v in config["rt_indicators"].items():

        if k == "rt_comparision":

            for day, day_rule in v["ratio"].items():

                df[day] = (
                    rt[
                        (
                            rt["last_updated"]
                            > (
                                rt["last_updated"].max()
                                - dt.timedelta(day_rule["delay"] + 7)
                            )
                        )
                        & (
                            rt["last_updated"]
                            < (
                                rt["last_updated"].max()
                                - dt.timedelta(day_rule["delay"])
                            )
                        )
                    ]
                    .groupby("city_id")
                    .agg({v["column"]: day_rule["agg"]})[v["column"]]
                )

            df[k] = np.where(
                df["rt_10days_week_max"] / df["rt_17days_week_avg"] > 1.1,
                "piorando",
                np.where(
                    df["rt_10days_week_max"] / df["rt_17days_week_avg"] > 0.9,
                    "estabilizando",
                    "melhorando",
                ),
            )
            df[k] = np.where(
                df["rt_10days_week_max"].isnull() | df["rt_17days_week_avg"].isnull(),
                np.nan,
                df[k],
            )

        else:
            df[k] = rt[
                rt["last_updated"]
                == (rt["last_updated"].max() - dt.timedelta(v["delay"]))
            ].set_index("city_id")[v["column"]]

        if k == "rt_classification":
            df[k] = np.where(
                df[k] > 1.2, "subindo", np.where(df[k] > 1, "estabilizando", "descendo")
            )

    df["last_updated_rt"] = rt["last_updated"].max()
    return df


def _get_subnotification_indicators(df, config):

    cases = get_cases.now(config)[
        [
            "city_id",
            "city",
            "state",
            "deaths",
            "active_cases",
            "notification_rate",
            "state_notification_rate",
            "last_updated",
        ]
    ]

    cases["last_updated"] = pd.to_datetime(cases["last_updated"])

    deaths_last_week = cases[
        cases["last_updated"] == (cases["last_updated"].max() - dt.timedelta(7))
    ].set_index("city_id")["deaths"]

    cases = cases[cases["last_updated"] == cases["last_updated"].max()].set_index(
        "city_id"
    )

    df["subnotification_rate"] = 1 - cases["notification_rate"]
    df["subnotification_last_mortality_ratio"] = (
        deaths_last_week / cases["active_cases"]
    )

    df["subnotification_rank"] = (
        df.loc[
            cases[cases["notification_rate"] != cases["state_notification_rate"]].index
        ]
        .groupby("state")["subnotification_rate"]
        .rank(method="first")
    )

    df["subnotification_place_type"] = np.where(
        df["subnotification_rank"].isnull(), "state", "city"
    )

    return df


def _get_indicators_inloco(df, config):

    inloco_cities = get_inloco_cities.now(config)
    inloco_cities["dt"] = pd.to_datetime(inloco_cities["dt"])

    inloco_cities = (
        inloco_cities.sort_values(["city_name", "state_name", "dt"])
        .groupby(["city_name", "state_name"])
        .rolling(7, 7, on="dt")["isolated"]
        .mean()
        .reset_index()
    )

    # TODO: +100 cidades que o nome não bate
    inloco_cities = (
        df.reset_index()[["city_id", "city_name", "state_name"]]
        .merge(inloco_cities, on=["city_name", "state_name"])
        .set_index("city_id")
    )

    results = pd.DataFrame(inloco_cities[["city_name", "state_name"]].drop_duplicates())

    results["inloco_today_7days_avg"] = inloco_cities[
        inloco_cities["dt"] == inloco_cities["dt"].max()
    ]["isolated"]

    results["inloco_last_week_7days_avg"] = inloco_cities[
        inloco_cities["dt"] == (inloco_cities["dt"].max() - dt.timedelta(7))
    ]["isolated"]

    results["inloco_comparision"] = np.where(
        results["inloco_today_7days_avg"] > results["inloco_last_week_7days_avg"],
        "subindo",
        np.where(
            results["inloco_today_7days_avg"] == results["inloco_last_week_7days_avg"],
            "estabilizando",
            "descendo",
        ),
    )

    results["last_updated_inloco"] = inloco_cities["dt"].max()

    return df.merge(results, how="left")


@allow_local
def now(config):

    config["rt_indicators"] = {
        "rt_10days_ago_low": {"delay": 10, "column": "Rt_low_95"},
        "rt_10days_ago_high": {"delay": 10, "column": "Rt_high_95"},
        "rt_17days_ago_low": {"delay": 17, "column": "Rt_low_95"},
        "rt_17days_ago_high": {"delay": 17, "column": "Rt_high_95"},
        "rt_classification": {"delay": 10, "column": "Rt_most_likely"},
        "rt_comparision": {
            "ratio": {
                "rt_10days_week_max": {"agg": "max", "delay": 10},
                "rt_17days_week_avg": {"agg": "mean", "delay": 17},
            },
            "column": "Rt_most_likely",
        },
    }

    df = get_simulacovid_main.now(config)[
        [
            "city_id",
            "city_name",
            "state",
            "state_name",
            "number_beds",
            "number_ventilators",
        ]
    ].set_index("city_id")

    df = _get_subnotification_indicators(df, config)
    df = _get_rt_indicators(df, config)
    df = _get_indicators_inloco(df, config)

    return df


TESTS = {
    "more than 5570 cities": lambda df: len(df["city_id"].unique()) <= 5570,
    "df is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
}
