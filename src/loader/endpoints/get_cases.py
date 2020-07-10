import pandas as pd
import datetime
import numpy as np
from urllib.request import Request, urlopen
import json
import time

import gzip
import io
from urllib.request import Request, urlopen

from endpoints.helpers import allow_local
from endpoints import get_places_id
from utils import download_from_drive, match_place_id


def _get_notification_ratio(df, config, place_col):
    """
    Calculate city/state notification rate = CFR * I(t) / D(t)
    
    1. Get daily 1/death_ratio = I(t) / D(t)
    2. Calculate mavg of 7 days = CFR / death_ratio
    """

    cfr = config["br"]["seir_parameters"]["fatality_ratio"]

    rate = (
        df.groupby([place_col, "last_updated"])
        .sum()
        .reset_index()
        .set_index("last_updated", drop=True)
        .groupby(place_col)
        .apply(lambda x: x["confirmed_cases"] / x["deaths"])
        .rolling(window=7, min_periods=1)
        .apply(lambda x: np.mean(x) * cfr, raw=True)
        .reset_index()
    )

    return rate


def _adjust_subnotification_cases(df, config):

    # Calcula taxa de notificação por cidade / estado
    df_city = _get_notification_ratio(df, config, "city_id").rename(
        {0: "city_notification_rate"}, axis=1
    )
    df_state = _get_notification_ratio(df, config, "state_id").rename(
        {0: "state_notification_rate"}, axis=1
    )

    df = df.merge(df_city, on=["city_id", "last_updated"]).merge(
        df_state, on=["state_id", "last_updated"]
    )

    # Escolha taxa de notificação para a cidade: caso sem mortes, usa taxa UF
    df["notification_rate"] = df["city_notification_rate"]
    df["notification_rate"] = np.where(
        df["notification_rate"].isnull(),
        df["state_notification_rate"],
        df["city_notification_rate"],
    )
    # Ajusta caso taxa > 1:
    df["notification_rate"] = np.where(
        df["notification_rate"] > 1, 1, df["notification_rate"]
    )

    df["state_notification_rate"] = np.where(
        df["state_notification_rate"] > 1, 1, df["state_notification_rate"]
    )

    return df[
        ["city_id", "state_notification_rate", "notification_rate", "last_updated"]
    ].drop_duplicates()


def _get_active_cases(df, window_period, cases_params):

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


# def _correct_cumulative_cases(df):

#     # Corrije acumulado para o valor máximo até a data
#     df["confirmed_cases"] = df.groupby("city_id").cummax()["confirmed_cases"]
#     df["deaths"] = df.groupby("city_id").cummax()["deaths"]

#     # Recalcula casos diários
#     df["daily_cases"] = df.groupby("city_id")["confirmed_cases"].diff(1)

#     # Ajusta 1a dia para o acumulado
#     df["daily_cases"] = np.where(
#         df["daily_cases"].isnull() == True, df["confirmed_cases"], df["daily_cases"]
#     )
#     return df


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
            (group[col] < group["previous_{}".format(col)]) & (group["is_zero"]),
            group["previous_{}".format(col)],
            group[col],
        )

        group[new] = group[col].diff(1)

        del group["previous_{}".format(col)]

    # del group["is_zero"]

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

        df = (
            _download_brasilio_table(config["br"]["cases"]["url"])
            .query("place_type == 'city'")
            .dropna(subset=["city_ibge_code"])
            .fillna(0)
            .rename(columns=config["br"]["cases"]["rename"])
            .assign(last_updated=lambda x: pd.to_datetime(x["last_updated"]))
            .sort_values(["city_id", "state_id", "last_updated"])
            .groupby("city_id")
            .apply(_correct_negatives)
            .pipe(_get_active_cases, infectious_period, config["br"]["cases"])
            .rename(columns=config["br"]["cases"]["rename"])
        )

        df = df.merge(
            _adjust_subnotification_cases(df, config), on=["city_id", "last_updated"]
        ).assign(
            active_cases=lambda x: np.where(
                x["notification_rate"].isnull(),
                round(x["infectious_period_cases"], 0),
                round(x["infectious_period_cases"] / x["notification_rate"], 0),
            ),
            city_id=lambda x: x["city_id"].astype(int),
        )

    # Fix places names & ids from default
    return match_place_id(
        df,
        get_places_id.now(config),
        {"city_name": "city_id", "state_name": "state_id", "state_num_id": "state_id"},
    )


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
