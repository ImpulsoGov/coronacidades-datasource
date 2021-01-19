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
    "delay_days": 19,  # daysBefore
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


def get_population(place_id):
    pop = pd.read_csv(
        Path(
            "endpoints/scripts/br_health_region_tabnet_age_dist_2019_treated.csv"
        ).resolve()
    ).assign(
        state_num_id=lambda df: df["health_region_id"].apply(lambda x: int(str(x)[:2]))
    )

    pop = pop.groupby(place_id).sum()
    pop.index = pop.index.astype(str)
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
    # Get region mortality prob weighted by age
    weighted_ifr_by_age = (
        get_population(place_id)
        .apply(lambda row: row / row["total"], axis=1)
        .drop(columns=["total"])
        .dot(pd.Series(ifr_by_age))
    )
    weighted_ifr_by_age.name = "expected_mortality"

    # Choose cases & deaths cols to use
    if not is_acum:
        cases = "daily_cases"
        deaths = "new_deaths"
    else:
        cases = "confirmed_cases"
        deaths = "deaths"

    # Agg cases and deaths mavg
    df = (
        df.groupby([place_id, "last_updated"])
        .agg(
            {cases: "sum", deaths: "sum"}
        )  # sum for all cities in the same health_region
        .reset_index()
    )

    df = (
        df.groupby(place_id)
        .rolling(
            agg_params["mavg_window"],
            min_periods=agg_params["mavg_window"],
            on="last_updated",
        )
        .mean()
        .reset_index(1, drop=True)
        .drop(columns=[place_id])  # it repets with index for some reason
        .reset_index()
    )

    # Join weighted ifr by age
    df[place_id] = df[place_id].astype(str)
    df = df.set_index(place_id).dropna().join(weighted_ifr_by_age).reset_index()

    # Add probable infected date col
    df["date_infected"] = df["last_updated"] - datetime.timedelta(
        days=agg_params["delay_days"]
    )
    # Estimate cases on delayed date
    df_estimation = df[[place_id, "date_infected", deaths, "expected_mortality"]]
    df_estimation["estimated_cases"] = df_estimation.apply(
        lambda row: bin_neg_simulation(row[deaths], row["expected_mortality"]), axis=1,
    )

    # Join notification rate on delayed date
    df = df.drop("date_infected", 1).merge(
        df_estimation.rename(columns={"date_infected": "last_updated"})[
            ["last_updated", place_id, "estimated_cases"]
        ],
        on=[place_id, "last_updated"],
    )

    # Set category type to optimize calculation
    df[place_id] = df[place_id].astype("category")

    # If using new_deaths, need to cumulative sum estimated cases
    if not is_acum:
        df["total_estimated_cases"] = df.groupby([place_id])["estimated_cases"].cumsum()

    # Get notification rate & fill zero with last value
    df["notification_rate"] = (df[cases] / df["estimated_cases"]).clip(0, 1)
    df["notification_rate"] = df["notification_rate"].replace(
        to_replace=0, method="ffill"
    )

    return df.drop(columns=[deaths, cases])
