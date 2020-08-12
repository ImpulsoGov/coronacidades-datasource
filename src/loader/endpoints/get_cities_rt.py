import pandas as pd
import datetime as dt
from scipy import stats as sps
from joblib import Parallel, delayed
from utils import get_cases_series
from endpoints import get_city_cases
from loguru import logger

from endpoints.helpers import allow_local
from endpoints import get_cases
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
    # Filtra > 14 dias para cálculo
    if len(group) < 14:
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
        df[[place_id, "last_updated", "active_cases", "confirmed_cases"]]
        .dropna(subset=["active_cases"])
        .groupby([place_id, "last_updated"])
        .agg({"active_cases": sum, "confirmed_cases": sum})
        .reset_index()
        .rename(columns={"last_updated": "dates"})
        .assign(dates=lambda df: pd.to_datetime(df["dates"]))
        .query("confirmed_cases >= 100")
    )

    # Calcula Rt com mavg de casos ativos
    rt = (
        df_cases.groupby(place_id)
        .rolling(7, min_periods=7, on="dates")["active_cases"]
        .mean()
        .reset_index()
        .rename(columns={"active_cases": "I"})
        .groupby(place_id)
        .apply(run_epiestim)
        .reset_index(0)
    )

    return rt.rename(columns={"dates": "last_updated"})


@allow_local
def now(config):

    # Import cases
    df = get_city_cases.now(config, "br")
    df["last_updated"] = pd.to_datetime(df["last_updated"])

    # Filter cities with deaths i.e. has subnotification!
    df = df[df["city_notification_place_type"] == "city"]

    # Filter more than 14 days
    df = get_cases_series(df, "city_id", config["br"]["rt_parameters"]["min_days"])

    # subs cidades com 0 casos -> 0.1 caso no periodo
    df = df.replace(0, 0.1)

    # Run in parallel
    return sequential_run(df, config, place_type="city_id")


TESTS = {
    "data is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    "dataframe has null data": lambda df: all(df.isnull().any() == False),
    "rt most likely outside confidence interval": lambda df: len(
        df[
            (df["Rt_most_likely"] >= df["Rt_high_95"])
            & (df["Rt_most_likely"] <= df["Rt_high_95"])
        ]
    )
    == 0,
    # "city has rt with less than 14 days": lambda df: all(
    #     df.groupby("city_id")["last_updated"].count() > 14
    # )
    # == True,
}
