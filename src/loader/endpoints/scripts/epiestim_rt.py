# CITIES RT

import pandas as pd
import datetime as dt

from endpoints.helpers import allow_local
from endpoints import get_cities_cases
import rpy2.robjects as ro
from rpy2.robjects.conversion import localconverter

from rpy2.robjects import pandas2ri

pandas2ri.activate()

from rpy2.robjects.packages import importr

eps = importr("EpiEstim")

# TODO: passar para config
dic = {
    "replace": {
        "Mean(R)": "Rt_most_likely",
        "Quantile.0.05(R)": "Rt_low_95",
        "Quantile.0.95(R)": "Rt_high_95",
    },
    "episestim_params": {"mean_si": 4.7, "std_si": 2.9, "mean_prior": 3},
}


def run_epiestim(group):

    # Remove valores nulos
    group = group.dropna(subset=["I"])
    # Filtra > 15 dias para cálculo
    if len(group) < 15:
        return
    # Filtra séries não negativas
    if any(group["I"] < 0):
        return

    # Converte para série em R
    with localconverter(ro.default_converter + pandas2ri.converter):
        infected_series = ro.conversion.py2rpy(group[["I"]])

    # Calcula Rt
    rt = dict(
        eps.estimate_R(
            infected_series,
            method="parametric_si",
            config=eps.make_config(
                mean_si=dic["episestim_params"]["mean_si"],
                std_si=dic["episestim_params"]["std_si"],
                mean_prior=dic["episestim_params"]["mean_prior"],
            ),
        ).items()
    )["R"]

    # Recupera coluna de datas - começa depois de 7 dias
    rt = (
        rt.rename(columns=dic["replace"])[dic["replace"].values()]
        .reset_index(drop=True)
        .join(group.iloc[7:]["dates"].reset_index(drop=True))
    )
    return rt


def get_rt(df, place_id):

    # Agrega e filtra a série de casos: > 100 casos confirmados
    df_cases = (
        df[[place_id, "last_updated", "daily_cases", "confirmed_cases"]]
        .dropna(subset=["daily_cases"])
        .groupby([place_id, "last_updated"])
        .agg({"daily_cases": sum, "confirmed_cases": sum})
        .reset_index()
        .rename(columns={"last_updated": "dates"})
        .assign(dates=lambda df: pd.to_datetime(df["dates"]))
        .query("confirmed_cases >= 100")
    )

    # Calcula Rt com mavg de casos ativos
    rt = (
        df_cases.groupby(place_id)
        .rolling(7, min_periods=7, on="dates")["daily_cases"]
        .mean()
        .reset_index()
        .rename(columns={"daily_cases": "I"})
        .groupby(place_id)
        .apply(run_epiestim)
        .reset_index(0)
        .rename(columns={"dates": "last_updated"})
    )

    # Calcula crescimento
    rt = get_cities_cases.get_mavg_indicators(
        rt, "Rt_most_likely", place_id, weighted=False
    )
    return rt


@allow_local
def now(config=None):
    return get_rt(get_cities_cases.now(), place_id="city_id")


TESTS = {
    "data is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    "dataframe has null data": lambda df: all(
        df[["Rt_most_likely", "Rt_high_95", "Rt_low_95"]].isnull().any() == False
    ),
    "rt most likely outside confidence interval": lambda df: len(
        df[
            (df["Rt_most_likely"] <= df["Rt_high_95"])
            & (df["Rt_most_likely"] >= df["Rt_low_95"])
        ]
    )
    == len(df),
    # "less than 14 days": lambda df: all(
    #     df.groupby("city_id")["last_updated"].count() > 14
    # )
    # == True,
}


# REGION

import pandas as pd
import numpy as np
from endpoints.helpers import allow_local
from endpoints import get_health_region_cases, get_cities_rt


@allow_local
def now(config=None):
    return get_cities_rt.get_rt(
        get_health_region_cases.now(), place_id="health_region_id"
    )


# TODO: review tests
TESTS = {
    "data is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    "dataframe has null data": lambda df: all(
        df[["Rt_most_likely", "Rt_high_95", "Rt_low_95"]].isnull().any() == False
    ),
    "rt most likely outside confidence interval": lambda df: len(
        df[
            (df["Rt_most_likely"] <= df["Rt_high_95"])
            & (df["Rt_most_likely"] >= df["Rt_low_95"])
        ]
    )
    == len(df),
    # "region has rt with less than 14 days": lambda df: all(
    #     df.groupby("health_region_id")["last_updated"].count() > 14
    # )
    # == True,
}


# STATES

import pandas as pd
import numpy as np
from endpoints.helpers import allow_local
from endpoints import get_states_cases, get_cities_rt


@allow_local
def now(config=None):
    # TODO: mudar para get_[cities/region/states]_cases quando tiver as tabelas
    return get_cities_rt.get_rt(get_states_cases.now(), place_id="state_num_id")


# TODO: review tests
TESTS = {
    "data is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    "dataframe has null data": lambda df: all(
        df[["Rt_most_likely", "Rt_high_95", "Rt_low_95"]].isnull().any() == False
    ),
    # "not all 27 states with updated rt": lambda df: len(
    #     df.drop_duplicates("state_num_id", keep="last")
    # )
    # == 27,
    "rt most likely outside confidence interval": lambda df: len(
        df[
            (df["Rt_most_likely"] <= df["Rt_high_95"])
            & (df["Rt_most_likely"] >= df["Rt_low_95"])
        ]
    )
    == len(df),
    # "state has rt with less than 14 days": lambda df: all(
    #     df.groupby("state_num_id")["last_updated"].count() > 14
    # )
    # == True,
}
