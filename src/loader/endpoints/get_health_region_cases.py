import pandas as pd
import numpy as np
from logger import logger

from endpoints import get_cities_cases
from endpoints.get_cities_cases import get_rolling_indicators, get_default_ids

from endpoints.helpers import allow_local

@allow_local
def now(config):
    cols = [
        "health_region_id",
        "health_region_name",
        "state_num_id",
        "state_id",
        "state_name",
        "last_updated",
    ]

    # Pega casos já tratados para cidades (+ taxa de notificação da regional)
    df = get_cities_cases.now(config)
    logger.info("FINISH LOAD DATA")

    # Agrega colunas calculadas em cidades para regionais de saúde
    grouped = df.groupby(cols, sort=False)
    df = grouped.agg(
        {
            "confirmed_cases": "sum",
            "daily_cases": "sum",
            "deaths": "sum",
            "new_deaths": "sum",
            "is_last": "max", # todas as cidades são atualizadas numa mesma tabela diária no Brasil.io
            "estimated_cases": "mean", # mesmo valor para todas cidades
            "expected_mortality": "mean",
            "notification_rate": "mean",
            "total_estimated_cases": "mean",
        }
    ).reset_index()
    
    # Converte data e ordena tabela
    df["last_updated"] = pd.to_datetime(df["last_updated"])
    df = df.sort_values(by=["health_region_id", "last_updated"])

    # Padroniza ids e nomes + populacao
    df = get_default_ids(df, config, place_type="health_region")
    logger.info("FINISH DATA TREATMENT")

    # Gera métricas de média móvel e tendência
    groups = df.groupby("health_region_id", as_index=False)
    df = groups.apply(
        lambda x: get_rolling_indicators(x, config, cols=["daily_cases", "new_deaths"])
    )
    df = df.reset_index(drop=True)
    logger.info("FINISH DATA GROW CALCULATION")

    # Calcula casos ativos
    df["active_cases"] = np.nan
    df.loc[~df["notification_rate"].isnull(), "active_cases"] = round(
        df["infectious_period_cases"] / df["notification_rate"], 0
    )

    return df

TESTS = {
    "more than 450 regions": lambda df: len(df["health_region_id"].unique()) <= 450,
    "df is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    "population not fixed by region": lambda df: len(
        df[["population", "health_region_id"]].drop_duplicates()
    )
    == len(df["health_region_id"].drop_duplicates()),
    "last date is repeated": lambda df: all(
        df.loc[df.groupby("health_region_id")["last_updated"].idxmax()][
            ["last_updated", "health_region_id"]
        ]
        == df[df["is_last"] == True][["last_updated", "health_region_id"]],
    ),
    "negative values on cumulative deaths or cases": lambda df: len(
        df[(df["confirmed_cases"] < 0) | (df["deaths"] < 0)]
    )
    == 0,
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