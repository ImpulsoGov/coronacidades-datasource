import pandas as pd
from logger import logger
import numpy as np

from endpoints.get_cities_cases import (
    download_brasilio_table,
    treat_df,
    get_default_ids,
    get_rolling_indicators,
    _get_notification_rate
)

from endpoints.helpers import allow_local

@allow_local
def now(config):
    cols = {
        "city": "object",
        "city_ibge_code": "object",
        "date": "object",
        "epidemiological_week": "int",
        "is_last": "bool",
        "is_repeated": "bool",
        "last_available_confirmed": "int",
        "last_available_date": "object",
        "last_available_death_rate": "float",
        "last_available_deaths": "int",
        "place_type": "object",
        "state": "object",
        "new_confirmed": "int",
        "new_deaths": "int",
    }

    # Baixa e carrega dados em memória
    df = pd.read_csv(
        download_brasilio_table(),
        usecols=cols.keys(),
        dtype=cols,
        parse_dates=["last_available_date", "date"],
    )
    logger.info("FULL DATA LOADED FROM BRASILIO")

    # Trata dados
    df = treat_df(df, config, place_type="state", place_id="state")
    # Padroniza ids e nomes
    df = get_default_ids(df, config, place_type="state")
    logger.info("FINISH DATA TREATMENT")

    # Gera métricas de média móvel e tendência
    groups = df.groupby("state_num_id", as_index=False)
    df = groups.apply(
        lambda x: get_rolling_indicators(x, config, cols=["daily_cases", "new_deaths"])
    )
    df = df.reset_index(drop=True)
    logger.info("FINISH DATA GROW CALCULATION")

    # Gera dados de taxa de notificacao e casos ativos
    df = _get_notification_rate(df, place_id="state_num_id")
    logger.info("FINISH NOTIFICATION RATE CALCULATION")

    # Calcula casos ativos
    df["active_cases"] = np.nan
    df.loc[~df["notification_rate"].isnull(), "active_cases"] = round(
        df["infectious_period_cases"] / df["notification_rate"], 0
    )

    return df


TESTS = {
    "not 27 states": lambda df: len(df["state_num_id"].unique()) == 27,
    "df is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    "population not fixed by state": lambda df: len(
        df[["population", "state_num_id"]].drop_duplicates()
    )
    == len(df["state_num_id"].drop_duplicates()),
    "last date is repeated": lambda df: all(
        df.loc[df.groupby("state_num_id")["last_updated"].idxmax()][
            ["last_updated", "state_num_id"]
        ]
        == df[df["is_last"] == True][["last_updated", "state_num_id"]],
    ),
    "negative values on cumulative deaths or cases": lambda df: len(
        df[(df["confirmed_cases"] < 0) | (df["deaths"] < 0)]
    )
    == 0,
}
