# Código contruído com o Instituto Serrapilheira
# ==============================================
# Código original: Prof. Tiago Pereira da Silva (ICMC-USP) e Kevin Kühl (ICMC/USP)
# Código para produção (adaptação): Fernanda Scovino e Victor Cortez (ImpulsoGov)
# ==============================================

import pandas as pd
from scipy.stats import nbinom
import scipy
import datetime
from pathlib import Path

# Params
ifr_by_age = {
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
    "delay_days": 14,  # daysBefore
}


# Funcs
def bin_neg_simulation(k, p, n=10000, odd=99):
    """
    Funcao que retorna o valor de uma distribuicao binomial negativa frente aos parametros dados
    """
    if k <= 0:
        return None
    a = 100 - odd
    b = odd + (100 - odd)
    r = nbinom.rvs(k, p, size=n)
    return int(
        scipy.stats.tmean(
            r,
            (scipy.stats.scoreatpercentile(r, a), scipy.stats.scoreatpercentile(r, b)),
        )
    )


def get_rolling(df, col):
    return df.rolling(
        agg_params["mavg_window"],
        min_periods=agg_params["mavg_window"],
        on="last_updated",
    )[col].mean()


def get_population(place_id):
    pop = (
        pd.read_csv(
            Path(
                "endpoints/scripts/br_health_region_tabnet_age_dist_2019_treated.csv"
            ).resolve()
        )
        .assign(
            state_num_id=lambda df: df["health_region_id"].apply(
                lambda x: int(str(x)[:2])
            )
        )
        .groupby(place_id)
        .sum()
    )
    return pop.drop(
        columns=[col for col in pop.columns if "_id" in col]
    )  # drop other place level


def now(df, place_id="health_region_id", is_acum=False):
    """
    Calcula a taxa de notificação de dados da regional/estado com base no algoritmo desenvolvido pelo Instituto Serrapilheira.

    Parameters
    ----------
    df: pd.DataFrame
        Dados acumulados ou novos de casos e mortes dos locais por dia
    place_id: string
        Nível de agregação populacional (regional = health_region_id, estado = state_num_id).  Default = health_region_id.
    is_acum: boolean
        Indica se os casos e mortes considerados são acumulados (False) ou novos diários (True). Default = False
    
    """
    # TODO: add state_num_id col!!
    # Get region mortality prob weighted by age
    weighted_ifr_by_age = (
        get_population(place_id)
        .apply(lambda row: row / row["total"], axis=1)
        .drop(columns=["total"])
        .dot(pd.Series(ifr_by_age))
    )

    weighted_ifr_by_age.name = "expected_mortality"

    # Choose cases & deaths cols
    if not is_acum:
        cases = "daily_cases"
        deaths = "new_deaths"
    else:
        cases = "confirmed_cases"
        deaths = "deaths"

    # Agg place data, get deaths mavg & add infected date col
    df = (
        df.groupby([place_id, "last_updated"])
        .agg({cases: "sum", deaths: "sum"})
        .reset_index(level=1)
        .assign(deaths_mavg=lambda df: get_rolling(df, deaths))
        .dropna()
        .join(weighted_ifr_by_age)
        .reset_index()
        .assign(
            last_updated=lambda df: pd.to_datetime(
                df["last_updated"], format="%Y-%m-%d"
            )
        )
        .assign(
            date_infected=lambda df: df["last_updated"]
            - datetime.timedelta(days=agg_params["delay_days"])
        )
    )

    # Estimate cases on delayed date
    df_estimation = df[[place_id, "date_infected", "deaths_mavg", "expected_mortality"]]
    df_estimation["estimated_cases"] = df_estimation.apply(
        lambda row: bin_neg_simulation(row["deaths_mavg"], row["expected_mortality"]),
        axis=1,
    )

    # Join notification rate on delayed date
    df = df.drop("date_infected", 1).merge(
        df_estimation.rename(columns={"date_infected": "last_updated"})[
            ["last_updated", place_id, "estimated_cases"]
        ],
        on=[place_id, "last_updated"],
    )

    # If using new_deaths, need to sum estimated cases
    if not is_acum:
        df = df.assign(
            total_estimated_cases=lambda df: df.groupby(place_id)[
                "estimated_cases"
            ].cumsum()
        )

    # Get notification rate
    df = df.assign(
        notification_rate=lambda df: (df[cases] / df["estimated_cases"]).clip(0, 1)
    )

    # df["notification_rate"] = df["confirmed_cases"] / df["estimated_cases"]
    # df["notification_rate"] = df["notification_rate"].clip(0, 1)
    return df.drop(columns=["new_deaths", "daily_cases"])
