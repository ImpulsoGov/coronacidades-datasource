import pandas as pd
import numpy as np
import datetime as dt
import math

from endpoints import get_cities_rt, get_cases, get_inloco_cities, get_simulacovid_main
from endpoints.helpers import allow_local
from endpoints.aux.simulator import run_simulation


# TODO: ajustar subnotificação para quando não tem do municipio
# def _fix_state_notification(row, states_rate):

#     if np.isnan(row["state_notification_rate"]):
#         return states_rate.loc[row["state_id"]].values[0]
#     else:
#         return row["state_notification_rate"]


def _calculate_recovered(df, params):

    confirmed_adjusted = int(
        df[["confirmed_cases"]].sum() / (1 - df["subnotification_rate"])
    )

    if confirmed_adjusted == 0:  # dont have any cases yet
        params["population_params"]["R"] = 0
        return params

    params["population_params"]["R"] = (
        confirmed_adjusted
        - params["population_params"]["I"]
        - params["population_params"]["D"]
    )

    if params["population_params"]["R"] < 0:
        params["population_params"]["R"] = (
            confirmed_adjusted - params["population_params"]["D"]
        )

    return params


def _prepare_simulation(row, config):

    params = {
        "population_params": {
            "N": row["population"],
            "I": [row["active_cases"] if not np.isnan(row["active_cases"]) else 1][0],
            "D": [row["deaths"] if not np.isnan(row["deaths"]) else 0][0],
        },
        "n_beds": row["number_beds"],
        "n_ventilators": row["number_ventilators"],
        "R0": {"best": row["rt_10days_ago_low"], "worst": row["rt_10days_ago_high"]},
    }

    # TODO: ajustar subnotificação para quando não tem do municipio
    if np.isnan(row["subnotification_rate"]):
        return np.nan, np.nan

    else:
        params = _calculate_recovered(row, params)

        dday_beds, _ = run_simulation(params, config)

        return dday_beds["best"], dday_beds["worst"]


def _get_indicators_capacity(df, config):

    df["dday_beds_best"], df["dday_beds_worst"] = zip(
        *df.apply(lambda row: _prepare_simulation(row, config), axis=1)
    )

    df["dday_classification"] = np.where(
        (df["dday_beds_best"].isnull()) | (df["dday_beds_best"].isnull()),
        "",
        np.where(
            df["dday_beds_worst"] > 60,
            "bom",
            np.where(df["dday_beds_best"] < 30, "ruim", "insatisfatório"),
        ),
    )

    return df


def _get_indicators_inloco(df, config):

    inloco_cities = get_inloco_cities.now(config)
    inloco_cities["dt"] = pd.to_datetime(inloco_cities["dt"])

    # Média móvel do distanciamento para cada 7 dias
    inloco_cities = (
        inloco_cities.sort_values(["city_name", "state_name", "dt"])
        .groupby(["city_name", "state_name"])
        .rolling(7, 7, on="dt")["isolated"]
        .mean()
        .reset_index()
    )

    # TODO: +100 cidades que o nome não bate - dados da inloco não tem city_id
    inloco_cities = (
        df.reset_index()[["city_id", "city_name", "state_name"]]
        .merge(inloco_cities, on=["city_name", "state_name"])
        .set_index("city_id")
    )

    config["inloco_indicators"] = {
        "inloco_today_7days_avg": {"delay": 0},
        "inloco_last_week_7days_avg": {"delay": 7},
        "inloco_comparision": {
            "cols": ["inloco_today_7days_avg", "inloco_last_week_7days_avg"]
        },
        "last_updated_inloco": None,
    }

    results = pd.DataFrame(inloco_cities[["city_name", "state_name"]].drop_duplicates())

    for k, v in config["inloco_indicators"].items():

        # Comparação da media da ultima semana
        if k == "inloco_comparision":
            results[k] = np.where(
                (results[v["cols"][0]].isnull()) | (results[v["cols"][1]].isnull()),
                "",
                np.where(
                    results[v["cols"][0]] > results[v["cols"][1]],
                    "subindo",
                    np.where(
                        results[v["cols"][0]] == results[v["cols"][1]],
                        "estabilizando",
                        "descendo",
                    ),
                ),
            )

        elif k == "last_updated_inloco":
            results[k] = inloco_cities["dt"].max()

        else:
            results[k] = inloco_cities[
                inloco_cities["dt"]
                == inloco_cities["dt"].max() - dt.timedelta(v["delay"])
            ]["isolated"]

    return df.join(results[list(config["inloco_indicators"].keys())])


def _get_indicators_rt(df, config):

    # Regra de indicadores da taxa de contágio: Taxa + recente de 10 dias atras para confiabilidade
    config["rt_indicators"] = {
        "rt_10days_ago_low": {"delay": 10, "column": "Rt_low_95"},
        "rt_10days_ago_high": {"delay": 10, "column": "Rt_high_95"},
        "rt_17days_ago_low": {"delay": 17, "column": "Rt_low_95"},
        "rt_17days_ago_high": {"delay": 17, "column": "Rt_high_95"},
        "rt_classification": {"delay": 10, "column": "Rt_most_likely"},
        "rt_comparision": {
            "ratio": {
                "rt_10days_week_avg": {"agg": "mean", "delay": 10},
                "rt_17days_week_avg": {"agg": "mean", "delay": 17},
            },
            "column": "Rt_most_likely",
        },
    }

    rt = get_cities_rt.now(config)
    rt["last_updated"] = pd.to_datetime(rt["last_updated"])

    for k, v in config["rt_indicators"].items():

        if k == "rt_comparision":

            for day, day_rule in v["ratio"].items():

                df[day] = (
                    rt[
                        (
                            rt["last_updated"]
                            > (
                                rt["last_updated"].max()
                                - dt.timedelta(day_rule["delay"] + 7)
                            )
                        )
                        & (
                            rt["last_updated"]
                            < (
                                rt["last_updated"].max()
                                - dt.timedelta(day_rule["delay"])
                            )
                        )
                    ]
                    .groupby("city_id")
                    .agg({v["column"]: day_rule["agg"]})[v["column"]]
                )

            # Comparação da média da ultima semana
            df[k] = np.where(
                (df["rt_10days_week_avg"].isnull())
                | (df["rt_17days_week_avg"].isnull()),
                "",
                np.where(
                    df["rt_10days_week_avg"] / df["rt_17days_week_avg"] > 1.1,
                    "piorando",
                    np.where(
                        df["rt_10days_week_avg"] / df["rt_17days_week_avg"] > 0.9,
                        "estabilizando",
                        "melhorando",
                    ),
                ),
            )

        # Outros indicadores: só pega o valor de 10 dias astras
        else:
            df[k] = rt[
                rt["last_updated"]
                == (rt["last_updated"].max() - dt.timedelta(v["delay"]))
            ].set_index("city_id")[v["column"]]

        # Classificação: é feita com o valor esperado (most_likely) calculado acima
        if k == "rt_classification":
            df[k] = np.where(
                df[k].isnull(),
                "",
                np.where(
                    df[k] > 1.2, "ruim", np.where(df[k] > 1, "instatisfatório", "bom"),
                ),
            )

    df["last_updated_rt"] = rt["last_updated"].max()

    return df


def _get_indicators_subnotification(df, config):

    cases = get_cases.now(config)[
        [
            "city_id",
            "city",
            "state",
            "deaths",
            "active_cases",
            "notification_rate",
            "state_notification_rate",
            "last_updated",
            "is_last",
        ]
    ]

    cases["last_updated"] = pd.to_datetime(cases["last_updated"])

    deaths_last_week = cases[
        cases["last_updated"] == (cases["last_updated"].max() - dt.timedelta(7))
    ].set_index("city_id")["deaths"]

    cases = cases[cases["is_last"] == True].set_index("city_id")

    df["subnotification_rate"] = 1 - cases["notification_rate"]

    # Taxa de mortalidade: mortes da ultima semana / casos ativos hoje
    df["subnotification_last_mortality_ratio"] = (
        deaths_last_week / cases["active_cases"]
    )

    # Ranking de subnotificação dos municípios para cada estado
    df["subnotification_rank"] = (
        df.loc[
            cases[cases["notification_rate"] != cases["state_notification_rate"]].index
        ]
        .groupby("state")["subnotification_rate"]
        .rank(method="first")
    )

    df["subnotification_place_type"] = np.where(
        df["subnotification_rank"].isnull(), "state", "city"
    )

    df["last_updated_subnotification"] = cases["last_updated"]

    return df


@allow_local
def now(config):

    df = get_simulacovid_main.now(config)[
        [
            "city_id",
            "city_name",
            "state",
            "state_name",
            "population",
            "number_beds",
            "number_ventilators",
            "confirmed_cases",
            "active_cases",
            "deaths",
        ]
    ].set_index("city_id")

    # Calcula indicadores, classificações e comparações
    df = _get_indicators_subnotification(df, config).replace("", np.nan)
    df = _get_indicators_rt(df, config).replace("", np.nan)
    df = _get_indicators_inloco(df, config).replace("", np.nan)
    df = _get_indicators_capacity(df, config).replace("", np.nan)

    # Classificação geral: Nível de alerta
    df["overall_alert"] = np.where(
        (df["rt_classification"] == "bom")
        & (df["rt_comparision"] == "melhorando")
        & (df["dday_classification"] == "bom")
        & (df["subnotification_rate"] < 0.5),
        "baixo",
        np.where(
            (df["rt_classification"] == "insatisfatorio")
            & (df["rt_comparision"] == "estabilizando")
            & (df["dday_classification"] == "bom")
            & (df["subnotification_rate"] < 0.5),
            "médio",
            np.where(
                (df["rt_classification"] == "ruim")
                | (df["rt_comparision"] == "piorando")
                | (df["dday_classification"] == "ruim")
                | (df["subnotification_rate"] >= 0.5),
                "alto",
                "",
            ),
        ),
    )

    return df.replace("", np.nan).reset_index()


TESTS = {
    "more than 5570 cities": lambda df: len(df["city_id"].unique()) <= 5570,
    "df is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    "subnotification rank for all cities with notification": lambda df: all(
        df[~df["subnotification_rank"].isnull()]["subnotification_place_type"] == "city"
    ),
}
