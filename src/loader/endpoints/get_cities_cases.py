import pandas as pd
import datetime
import numpy as np
import json
import time

import gzip
import io
from urllib.request import Request, urlopen

from endpoints.helpers import allow_local
from endpoints import get_places_id
from endpoints.scripts import get_notification_rate
from utils import download_from_drive


def get_infectious_period_cases(df, window_period, cases_params, place_id):

    # Soma casos diários dos últimos dias de progressão da doença
    daily_active_cases = (
        df.set_index("last_updated")
        .groupby(place_id)["daily_cases"]
        .rolling(min_periods=1, window=window_period)
        .sum()
        .reset_index()
    )
    df = df.merge(
        daily_active_cases, on=[place_id, "last_updated"], suffixes=("", "_sum")
    ).rename(columns=cases_params["rename"])

    return df


def _get_growth(group):
    if group["diff_5_days"].values == 5:
        return "crescendo"
    elif group["diff_14_days"].values == -14:
        return "decrescendo"
    else:
        return "estabilizando"


def get_mavg_indicators(df, col, place_id, weighted=True):

    df = df.sort_values([place_id, "last_updated"])

    if weighted:
        divide = df["estimated_population_2019"] / 10 ** 5
    else:
        divide = 1

    # Cria coluna mavg
    df_mavg = df.assign(
        mavg=lambda df: df.groupby(place_id)
        .rolling(7, window_period=7, on="last_updated")[col]
        .mean()
        .round(1)
        .reset_index(drop=True)
    )

    df_mavg = df_mavg.assign(mavg_100k=lambda df: df["mavg"] / divide)

    # Cria colunas auxiliares para tendencia
    df_mavg = (
        df_mavg.assign(
            diff=lambda df: np.sign(df.groupby(place_id)["mavg_100k"].diff())
        )
        .assign(
            diff_5_days=lambda df: df.groupby(place_id)
            .rolling(5, window_period=5, on="last_updated")["diff"]
            .sum()
            .reset_index(drop=True)
        )
        .assign(
            diff_14_days=lambda df: df.groupby(place_id)
            .rolling(14, window_period=14, on="last_updated")["diff"]
            .sum()
            .reset_index(drop=True)
        )
    )

    # Calcula tendência
    df_mavg = df_mavg.assign(
        growth=lambda df: df.groupby([place_id, "last_updated"])
        .apply(_get_growth)
        .reset_index(drop=True)
    )

    return df.merge(
        df_mavg[["mavg", "mavg_100k", "growth", place_id, "last_updated"]],
        on=[place_id, "last_updated"],
    ).rename(
        columns={
            "mavg": col + "_mavg",
            "mavg_100k": col + "_mavg_100k",
            "growth": col + "_growth",
        }
    )


def correct_negatives(group):

    # Identify days not filled
    group["is_zero"] = np.where(
        (group["confirmed_cases"] == 0) & (group["deaths"] == 0), 1, 0
    )

    cols = {"confirmed_cases": "daily_cases", "deaths": "new_deaths"}

    # Get previous day of total cases & deaths when not filled
    for col, new in cols.items():

        group["previous_{}".format(col)] = group[col].shift(1)

        group[col] = np.where(
            (group[col] < group["previous_{}".format(col)]) & (group["is_zero"] == 1),
            group["previous_{}".format(col)],
            group[col],
        )

        group[new] = group[col].diff(1).fillna(group[col])

        del group["previous_{}".format(col)]

    del group["is_zero"]
    return group


def get_until_last(group):
    """Filter data until last date collected by group
    """
    group = group.sort_values("last_updated").reset_index()
    last = group[group["is_last"] == True]

    return group[group["last_updated"] <= last["last_updated"].values[0]]


def download_brasilio_table(url):
    response = urlopen(Request(url, headers={"User-Agent": "python-urllib"}))
    return pd.read_csv(io.StringIO(gzip.decompress(response.read()).decode("utf-8")))


@allow_local
def now(config, country="br"):

    if country == "br":

        infectious_period = (
            config["br"]["seir_parameters"]["severe_duration"]
            + config["br"]["seir_parameters"]["critical_duration"]
        )

        # Filter and rename columns
        df = (
            download_brasilio_table(config["br"]["cases"]["url"])
            .query("place_type == 'city'")
            .dropna(subset=["city_ibge_code"])
            .fillna(0)
            .rename(columns=config["br"]["cases"]["rename"])
            .assign(last_updated=lambda x: pd.to_datetime(x["last_updated"]))
            .sort_values(["city_id", "state_id", "last_updated"])
            .groupby("city_id")
            .apply(lambda group: get_until_last(group))
            .reset_index(drop=True)
        )

        # Fix places name and ID
        places_ids = get_places_id.now(config).assign(
            city_id=lambda df: df["city_id"].astype(int)
        )

        df = (
            df.drop(["city_name"], 1)
            .assign(city_id=lambda df: df["city_id"].astype(int))
            .merge(
                places_ids[
                    [
                        "city_id",
                        "city_name",
                        "health_region_name",
                        "health_region_id",
                        "state_name",
                        "state_num_id",
                    ]
                ],
                on="city_id",
            )
        )

        # Correct negative values, get infectious period cases and get median of new cases
        df = (
            df.groupby("city_id")
            .apply(correct_negatives)
            .pipe(
                get_infectious_period_cases,
                infectious_period,
                config["br"]["cases"],
                "city_id",
            )
            .rename(columns=config["br"]["cases"]["rename"])
        )

        # Get indicators of mavg & growth
        df = get_mavg_indicators(df, "daily_cases", place_id="city_id")
        df = get_mavg_indicators(df, "new_deaths", place_id="city_id")

        # Get notification rates & active cases on date
        df = df.merge(
            get_notification_rate.now(df, "health_region_id"),
            on=["health_region_id", "last_updated"],
            how="left",
        ).assign(
            active_cases=lambda x: np.where(
                x["notification_rate"].isnull(),
                np.nan,  # round(x["infectious_period_cases"], 0),
                round(x["infectious_period_cases"] / x["notification_rate"], 0),
            ),
            city_id=lambda x: x["city_id"].astype(int),
        )

    return df


TESTS = {
    "more than 5570 cities": lambda df: len(df["city_id"].unique()) <= 5570,
    "df is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    "last date is repeated": lambda df: all(
        df.loc[df.groupby("city_id")["last_updated"].idxmax()][
            ["last_updated", "city_id"]
        ]
        == df[df["is_last"] == True][["last_updated", "city_id"]]
    )
    # TODO: corrigir teste! => ultima taxa calculada 14 dias antes
    # "notification_rate == NaN": lambda df: len(
    #     df[(df["notification_rate"].isnull() == True) & (df["is_last"] == True)].values
    # )
    # == 0,
    # "max(confirmed_cases) != max(date)": lambda df: all(
    # (df.groupby("city_id").max()["confirmed_cases"] \
    #  == df.query("is_last==True").set_index("city_id").sort_index()["confirmed_cases"]).values),
    # "max(deaths) != max(date)": lambda df: all(
    # (df.groupby("city_id").max()["deaths"] \
    #  == df.query("is_last==True").set_index("city_id").sort_index()["deaths"]).values)
}
