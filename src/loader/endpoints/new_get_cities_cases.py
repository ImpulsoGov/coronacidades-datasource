import os
import requests
import yaml
import gzip
import io
import pandas as pd
import numpy as np
​from brasilio import BrasilIO
from endpoints.helpers import allow_local

def download_brasilio_table(dataset="covid19", table_name="caso_full"):
    """
    Baixa dados completos do Brasil.io e retorna CSV.
    """
    api = BrasilIO()
    response = api.download(dataset, table_name)
    return io.TextIOWrapper(gzip.GzipFile(fileobj=response), encoding="utf-8")
​
​
def treat_df(df, place_type='city'):
    """
    Filtra dados exclusivos de municípios ou estados até a última df de atualização, e transforma negativos para zero.
    
    Args:
        place_type (str): ['city'|'state']
    """
    
    # Downcast dos tipos
    ints = df.select_dtypes(include=['int64','int32','int16']).columns
    df[ints] = df[ints].apply(pd.to_numeric, downcast='integer')
​
    floats = df.select_dtypes(include=['float']).columns
    df[floats] = df[floats].apply(pd.to_numeric, downcast='float')
    
    # Filtra por nivel geografico
    df = df.dropna(subset = ['city_ibge_code', place_type])
    df["city_ibge_code"] = df["city_ibge_code"].cat.remove_unused_categories()
    df = df.sort_values(["city_ibge_code", "last_available_date"])
    
    # Filtra até ultima df
    last_updated = df[df["is_last"] == True].set_index("city_ibge_code")["date"]
    groups = df.groupby("city_ibge_code")
    idxs = groups.apply(lambda group: group["date"] <= last_updated[group.name])
    df = df.loc[idxs[idxs == True].index.get_level_values(level=1)]
    
    # Transforma negativos para zero
    df.loc[df['new_confirmed'] < 0, "new_confirmed"] = 0
    df.loc[df['new_deaths'] < 0, "new_deaths"] = 0
    
    return df
​
def get_default_ids(df):
    """"
    Fix places name & ID and get total population
    """
    
    cols = {
        "city_id": "category",
        "city_name": "category",
        "health_region_name": "category",
        "health_region_id": "category",
        "state_name": "category",
        "state_num_id": "category",
        "population": "int"
    }
    
    places_ids = pd.read_csv("http://datasource.coronacidades.org/br/cities/cnes",
                             usecols=cols.keys(), dtype=cols)
​
    df = df.drop(columns=["city_name"]).merge(places_ids, on="city_id")
    return df
​
def get_rolling_indicators(group, cols=["daily_cases"], weighted=True):
    """
    Calcula variáveis dependentes do tempo: média móvel de casos, soma de casos nos dias de progressão da doença, 
    """
    
    divide = group["population"].values[0] / 10 ** 5 if weighted else 1
    
    group = group.set_index("last_updated")
    
    for col in cols:
        # Soma de casos nos dias de progressão da doença (para calculo de casos ativos)
        if col == "daily_cases":
            infectious_period = (
                config["br"]["seir_parameters"]["severe_duration"]
                + config["br"]["seir_parameters"]["critical_duration"]
            )
​
            group["infectious_period_cases"] = group[col].rolling(window=14, min_periods=1).sum()
​
        # Calcula média móvel
        group[f"{col}_mavg"] = group[col].rolling(window=7, min_periods=7).mean().round(1)
        group[f"{col}_mavg_100k"] = group[f"{col}_mavg"] / divide
​
        # Calcula tendência
        group[f"{col}_diff_14_days"] = np.sign(group[f"{col}_mavg"].diff()).rolling(14, min_periods=14).sum()
​
        group[f"{col}_growth"] = "estabilizando"
        group.loc[lambda x: x[f"{col}_diff_14_days"] >= 5, f"{col}_growth"] = "crescendo"
        group.loc[lambda x: x[f"{col}_diff_14_days"] <= -14, f"{col}_growth"] = "decrescendo"
    
    return group.reset_index()
​
​
def get_active_cases(df):
    """
    Get notification rates & active cases on date
    """
​
    df = df.merge(
        get_notification_rate.now(df, "health_region_id"),
        on=["health_region_id", "last_updated"],
        how="left",
    )
    
    df["active_cases"] = np.nan
    df.loc[df["notification_rate"].isnull(), "active_cases"] = round(df["infectious_period_cases"] / df["notification_rate"], 0)
    
    return df
​
​
# Definindo somente para rodar fora da pipeline, pois essa funcao fica em src/loader/utils.py
def get_config(url=os.getenv("CONFIG_URL")):
    return yaml.load(requests.get(url).text, Loader=yaml.FullLoader)
​
CONFIG_URL="https://raw.githubusercontent.com/ImpulsoGov/farolcovid/stable/src/configs/config.yaml"
config = get_config(CONFIG_URL)
​
​@allow_local
def now(config):
​
    cols = {
        'city': 'category',
        'city_ibge_code': 'category',
        'date': 'object',
        'epidemiological_week': 'int',
        'is_last': 'bool',
        'is_repeated': 'bool',
        'last_available_confirmed': 'int',
        'last_available_date': 'object',
        'last_available_death_rate': 'float',
        'last_available_deaths': 'int',
        'place_type': 'category',
        'state': 'category',
        'new_confirmed': 'int',
        'new_deaths': 'int'
    }
​
    Baixa e carrega dados em memória 
    df = pd.read_csv(download_brasilio_table(), 
                    usecols=cols.keys(), 
                    dtype=cols, 
                    parse_dates=["last_available_date", "date"])
    # Trata dados
    df = treat_df(df)
    df = df.rename(columns=config["br"]["cases"]["rename"])
    # Padroniza ids e nomes
    df = get_default_ids(df)
    # Gera métricas de média móvel e tendência 
    groups = df.groupby("city_id", as_index=False)
    df = groups.progress_apply(lambda x: get_rolling_indicators(x, cols=["daily_cases", "new_deaths"]))
    df = df.reset_index(drop=True)
    # Gera dados de taxa de notificacao e casos ativos -> PUXA DE get_notification_rate.now(df, "health_region_id")
    df = get_active_cases(df)
    return df
​
​
TESTS = {
    "more than 5570 cities": lambda df: len(df["city_id"].unique()) <= 5570,
    "df is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    "last date is repeated": lambda df: all(
        df.loc[df.groupby("city_id")["last_updated"].idxmax()][
            ["last_updated", "city_id"]
        ]
        == df[df["is_last"] == True][["last_updated", "city_id"]],
    ),
    "negative values on cumulative deaths or cases": lambda df: len(df[(df["last_available_confirmed"] < 0) | (df["last_available_deaths"] < 0)]) != 0
}
