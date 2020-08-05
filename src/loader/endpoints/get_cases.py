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
from utils import download_from_drive


def _calculate_notification_rate(df, config, place_col, rate_col):
    """
    Calculate place notification rate = CFR * I(t) / D(t)
    
    1. Get daily 1/death_ratio = I(t) / D(t)
    2. Calculate mavg of 7 days = CFR / death_ratio
    """

    cfr = config["br"]["seir_parameters"]["fatality_ratio"]

    # 1. Get daily 1/death_ratio = I(t) / D(t)
    df = (
        df.sort_values([place_col, "last_updated"])
        .groupby([place_col, "last_updated"])
        .sum()[["confirmed_cases", "deaths"]]
        .assign(rate=lambda df: df["confirmed_cases"] / df["deaths"])
        .reset_index()
        .rename(columns={"rate": rate_col})
        .drop(["confirmed_cases", "deaths"], 1)
    )

    # 2. Calculate mavg of 7 days = CFR / death_ratio
    df = (
        df.groupby(place_col)[[rate_col, "last_updated"]]
        .rolling(window=7, min_periods=7, on="last_updated")
        .mean()
        .reset_index(level=0)
    )

    df[rate_col] = df[rate_col].apply(lambda x: x * cfr)
    return df


def _get_notification_rates(df, config):

    # Calcula taxa de notificação por cidade / regiao / estado
    dic = {
        "city_id": "city_notification_rate",
        "health_region_id": "health_region_notification_rate",
        "state_id": "state_notification_rate",
    }
    for place, new_col in dic.items():
        df = df.merge(
            _calculate_notification_rate(df, config, place, new_col),
            on=[place, "last_updated"],
        )

    # Ajusta taxas de notificação da regional: quando não tem, usa a do estado!
    df["health_region_notification_place_type"] = (
        df[~df["state_notification_rate"].isnull()]["health_region_notification_rate"]
        .isnull()
        .map({True: "state", False: "health_region"})
    )

    df["health_region_notification_rate"] = df[
        "health_region_notification_rate"
    ].fillna(df["state_notification_rate"])

    # Ajusta taxas de notificação do municipio: quando não tem, usa a da regional!
    df["city_notification_place_type"] = (
        df[~df["state_notification_rate"].isnull()]["city_notification_rate"]
        .isnull()
        .map({False: "city"})
        .fillna(df["health_region_notification_place_type"])
    )

    df["notification_rate"] = df["city_notification_rate"].fillna(
        df["health_region_notification_rate"]
    )

    # Ajusta caso taxa > 1:
    rates_cols = [
        "notification_rate",
        "city_notification_rate",
        "health_region_notification_rate",
        "state_notification_rate",
    ]
    for col in rates_cols:
        df[col] = np.where(df[col] > 1, 1, df[col])

    return df[
        [
            "city_id",
            "last_updated",
            "city_notification_place_type",
            "health_region_notification_place_type",
        ]
        + rates_cols
    ].drop_duplicates()


def _get_infectious_period_cases(df, window_period, cases_params):

    # Soma casos diários dos últimos dias de progressão da doença
    daily_active_cases = (
        df.set_index("last_updated")
        .groupby("city_id")["daily_cases"]
        .rolling(min_periods=1, window=window_period)
        .sum()
        .reset_index()
    )

    df = df.merge(
        daily_active_cases, on=["city_id", "last_updated"], suffixes=("", "_sum")
    ).rename(columns=cases_params["rename"])

    return df


def _correct_negatives(group):

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


def _download_brasilio_table(url):
    response = urlopen(Request(url, headers={"User-Agent": "python-urllib"}))
    return pd.read_csv(io.StringIO(gzip.decompress(response.read()).decode("utf-8")))


@allow_local
def now(config, country="br"):

    if country == "br":

        infectious_period = (
            config["br"]["seir_parameters"]["severe_duration"]
            + config["br"]["seir_parameters"]["critical_duration"]
        )

        # Get data & clean table
        df = (
            _download_brasilio_table(config["br"]["cases"]["url"])
            .query("place_type == 'city'")
            .dropna(subset=["city_ibge_code"])
            .fillna(0)
            .rename(columns=config["br"]["cases"]["rename"])
            .assign(last_updated=lambda x: pd.to_datetime(x["last_updated"]))
            .sort_values(["city_id", "state_id", "last_updated"])
        )

        # Fix places_ids
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

        # Correct negative values & get infectious period cases
        df = (
            df.groupby("city_id")
            .apply(_correct_negatives)
            .pipe(
                _get_infectious_period_cases, infectious_period, config["br"]["cases"]
            )
            .rename(columns=config["br"]["cases"]["rename"])
        )

        # Get notification rates & active cases on date
        df = df.merge(
            _get_notification_rates(df, config), on=["city_id", "last_updated"]
        ).assign(
            active_cases=lambda x: np.where(
                x["notification_rate"].isnull(),
                round(x["infectious_period_cases"], 0),
                round(x["infectious_period_cases"] / x["notification_rate"], 0),
            ),
            city_id=lambda x: x["city_id"].astype(int),
        )

    return df


TESTS = {
    "more than 5570 cities": lambda df: len(df["city_id"].unique()) <= 5570,
    "df is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    "notification_rate == NaN": lambda df: len(
        df[(df["notification_rate"].isnull() == True) & (df["is_last"] == True)].values
    )
    == 0,
    "state_notification_rate == NaN": lambda df: len(
        df[
            (df["state_notification_rate"].isnull() == True) & (df["is_last"] == True)
        ].values
    )
    == 0,
    # TODO: test it before update to master
    # "max(confirmed_cases) != max(date)": lambda df: all(
    # (df.groupby("city_id").max()["confirmed_cases"] \
    #  == df.query("is_last==True").set_index("city_id").sort_index()["confirmed_cases"]).values),
    # "max(deaths) != max(date)": lambda df: all(
    # (df.groupby("city_id").max()["deaths"] \
    #  == df.query("is_last==True").set_index("city_id").sort_index()["deaths"]).values)
}
