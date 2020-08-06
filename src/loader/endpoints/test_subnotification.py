import pandas as pd
from scipy.stats import nbinom
import scipy
import datetime
import numpy as np
from endpoints import get_cases

# faixaEtaria = ["from_0_to_9", "from_10_to_19", "from_20_to_29", "from_30_to_39", "from_40_to_49", "from_50_to_59", "from_60_to_69", "from_70_to_79", "from_80_to_older"]

# Params
mortality_rate_by_age = {
    "from_0_to_9": 0.00002,
    "from_10_to_19": 0.00006,
    "from_20_to_29": 0.0003,
    "from_30_to_39": 0.0008,
    "from_40_to_49": 0.0015,
    "from_50_to_59": 0.006,
    "from_60_to_69": 0.022,
    "from_70_to_79": 0.051,
    "from_80_to_older": 0.093,
}

simulation_params = {
    "n": 10000,  # n
    "odd": 99,  # odd
}

agg_params = {
    "mavg_window": 15,  # sizeOfSlidingWindow
    "delay_days": 19,  # daysBefore
}


# Funcs
def bin_neg_simulation(k, p, n=10000, odd=99):
    """
    Funcao que retorna o valor de uma distribuicao binomial negativa frente aos parametros dados
    """
    a = 100 - odd
    b = odd + (100 - odd)
    r = nbinom.rvs(k, p, size=n)

    return int(
        scipy.stats.tmean(
            r,
            (scipy.stats.scoreatpercentile(r, a), scipy.stats.scoreatpercentile(r, b)),
        )
    )


def now(config):

    # TODO: add to drive paths on config
    # Get region mortality prob weighted by age
    region_mortality = (
        pd.read_csv("data/treated/br_health_region_tabnet_age_dist_2019_treated.csv")
        .set_index("health_region_id")
        .drop(columns=["health_region_name"])
        .apply(lambda row: row / row["total"], axis=1)
        .drop(columns=["total"])
        .dot(pd.Series(mortality_rate_by_age))
    )
    region_mortality.name = "expected_mortality"

    # Get region cases & add mortality prob
    df = (
        pd.read_csv("http://datasource.coronacidades.org/br/cities/cases/full")
        .groupby(["health_region_id", "last_updated"])
        .agg({"confirmed_cases": "sum", "deaths": "sum"})
        .assign(deaths_mavg=lambda df: get_rolling(df))
        .reset_index(level=1)
        .join(region_mortality)
        .reset_index()
        .assign(last_updated=lambda df: pd.to_datetime(df["last_updated"], format="%Y-%m-%d"))       
    )

    # TODO: delay de 19 dias?
    # Get deaths series
    df["deaths_agg"] = (
        df["deaths"]
        .groupby("health_region_id")
        .rolling(agg_params["mavg_window"], min_periods=1)
        .sum()
    )

    # for codigo in codigosCidades:
    #     # Separa as linhas referentes a uma dada cidade
    #     rows = dfDadosCasos.loc[dfDadosCasos["health_region_id"] == codigo]
    #     # Converte a data para o formato padrao
    #     rows["last_updated"] = pd.to_datetime(rows["last_updated"], format="%Y-%m-%d")
    #     # Ordena os valores por data
    #     rows = rows.sort_values(by="last_updated")
    #     # Obtem o numero de mortes acumuladas dentro de uma janela de tamanho definido acima (sizeOfSlidingWindow)
    #     # rows["accumulatedDeathsWindow"] = rows["dailyDeaths"].rolling(sizeOfSlidingWindow, min_periods=1).sum()
    #     # Volta "daysBefore" dias na data analisada (atraso entre contaminacao e confirmacao)
    #     rows["dateInfected"] = rows["last_updated"] - datetime.timedelta(days=daysBefore)
    #     rows["dateInfected"] = rows["dateInfected"]
    #     # Separa os dados para insercao
    #     frames = [finalDadosCovid, rows]
    #     # Insere no DataFrame final
    #     finalDadosCovid = pd.concat(frames, ignore_index=True)

    # Inicializa lista que contera os valores de estimativas diarias de contaminacao
    infectedList = []

    df["subnotification"] = df.apply(lambda row: bin_neg_simulation(row["deaths_agg"],))

    # Passa por toda base
    for index, rows in finalDadosCovid.iterrows():
        # Realiza a simulacao para obter o valor estimado de contaminados
        if rows["deaths"] > 0:
            infectedList.append(
                simulacao(
                    simulation_params["n"],
                    simulation_params["odd"],
                    rows["deaths"],
                    mortalidadeGeralCidade[rows["health_region_id"]],
                )
            )
        # Caso o numero de mortes seja 0, retorna 0
        else:
            infectedList.append(np.nan)

    # Insere os valores calculados
    finalDadosCovid["estimatedInfected"] = infectedList

