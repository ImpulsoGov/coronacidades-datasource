import pandas as pd
import numpy as np
import datetime as dt
import yaml

from endpoints import get_simulacovid_main, get_cases, get_cities_rt, get_inloco_cities
from endpoints.helpers import allow_local
from endpoints.aux.simulator import run_simulation


def _get_levels(df, rules):
    return pd.cut(
        df[rules["column_name"]],
        bins=rules["cuts"],
        labels=rules["categories"],
        right=False,
        include_lowest=True,
    )


def _calculate_recovered(df, params):  # ok

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


def _prepare_simulation(row, config):  # ok

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

    params = _calculate_recovered(row, params)
    dday_beds, _ = run_simulation(params, config)

    if row["subnotification_place_type"] == "state":
        return np.nan, np.nan
    else:
        return dday_beds["best"], dday_beds["worst"]


def get_indicators_capacity(df, config, rules, classify):  # ok

    df["dday_beds_best"], df["dday_beds_worst"] = zip(
        *df.apply(lambda row: _prepare_simulation(row, config), axis=1)
    )

    df["dday_beds_best"] = df["dday_beds_best"].replace(-1, 91)
    df["dday_beds_worst"] = df["dday_beds_worst"].replace(-1, 91)

    # Classificação: numero de dias para acabar a capacidade
    df[classify] = _get_levels(df, rules[classify])

    return df


def get_indicators_inloco(df, data, place_id, rules, growth):  # ok

    data["dt"] = pd.to_datetime(data["dt"])

    df["last_updated_inloco"] = data["data_last_refreshed"].max()

    if place_id == "city_id":
        group = ["city_name", "state_name"]

    if place_id == "state_id":
        group = ["state_name"]

    # Média móvel do distanciamento para cada 7 dias
    data = (
        data.sort_values(group + ["dt"])
        .groupby(group)
        .rolling(7, 7, on="dt")["isolated"]
        .mean()
        .reset_index()
    )

    # InLoco não tem ID! TODO: +100 cidades que o nome não bate
    data = (
        df.reset_index()[[place_id] + group].merge(data, on=group).set_index(place_id)
    )

    # Valores de referência: média da semana, média da ultima semana
    df["inloco_today_7days_avg"] = data[data["dt"] == data["dt"].max()].sort_values(
        place_id
    )["isolated"]

    df["inloco_last_week_7days_avg"] = data[
        data["dt"] == (data["dt"].max() - dt.timedelta(7))
    ].sort_values(place_id)["isolated"]

    # Crescimento: Comparação das médias
    df["inloco_ratio_week_avgs"] = df.apply(
        lambda row: row["inloco_today_7days_avg"] / row["inloco_last_week_7days_avg"],
        axis=1,
    )

    df[growth] = _get_levels(df, rules[growth])

    return df


def get_indicators_rt(df, data, place_id, rules, classify, growth):  # ok

    data["last_updated"] = pd.to_datetime(data["last_updated"])

    # Filtro: Rt considerado até 10 dias para confiabilidade do cálculo
    data = data[data["last_updated"] <= (data["last_updated"].max() - dt.timedelta(10))]

    df["last_updated_rt"] = data["data_last_refreshed"].max()

    # Min-max do Rt de 10 dias e 17 dias atrás
    df[["rt_10days_ago_low", "rt_10days_ago_high", "rt_10days_ago_most_likely"]] = (
        data[data["last_updated"] == data["last_updated"].max()]
        .sort_values(place_id)
        .set_index(place_id)[["Rt_low_95", "Rt_high_95", "Rt_most_likely"]]
    )

    df[["rt_17days_ago_low", "rt_17days_ago_high", "rt_17days_ago_most_likely"]] = (
        data[data["last_updated"] == (data["last_updated"].max() - dt.timedelta(7))]
        .sort_values(place_id)
        .set_index(place_id)[["Rt_low_95", "Rt_high_95", "Rt_most_likely"]]
    )

    # Classificação: melhor estimativa do Rt de 10 dias (rt_most_likely)
    df[classify] = _get_levels(df, rules[classify])

    # Evolução: média da ultima semana
    data = (
        data.sort_values([place_id, "last_updated"])
        .groupby([place_id])
        .rolling(7, min_periods=7, on="last_updated")["Rt_most_likely"]
        .mean()
        .reset_index()
    )

    df["rt_10days_ago_avg"] = (
        data[data["last_updated"] == data["last_updated"].max()]
        .sort_values(place_id)
        .set_index(place_id)["Rt_most_likely"]
    )

    df["rt_17days_ago_avg"] = (
        data[data["last_updated"] == (data["last_updated"].max() - dt.timedelta(7))]
        .sort_values(place_id)
        .set_index(place_id)["Rt_most_likely"]
    )

    df["rt_ratio_week_avg"] = df.apply(
        lambda row: row["rt_10days_ago_avg"] / row["rt_17days_ago_avg"], axis=1
    )

    # Crescimento: comparação da média da ultima semana
    df[growth] = _get_levels(df, rules[growth])

    # Classificação: considerando somente os que tem dados há 14 dias
    df.loc[df["rt_ratio_week_avg"].isnull(), classify] = np.nan

    return df


def _get_mortality_ratio(row):

    if row["deaths"] == 0:
        return np.nan
    elif row["confirmed_cases"] > 0:
        return row["deaths"] / row["confirmed_cases"]
    else:
        return np.nan


def _get_subnotification_rank(df, mask, place_id):

    if place_id == "city_id":
        return df[mask].groupby("state_id")["subnotification_rate"].rank(method="first")
        
    if place_id == "state":
        return df["subnotification_rate"].rank(method="first")


def get_indicators_subnotification(df, data, place_id, rules, classify):  # ok

    data["last_updated"] = pd.to_datetime(data["last_updated"])
    df["last_updated_subnotification"] = data["data_last_refreshed"].max()

    if place_id == "city_id":

        mask = (df["notification_rate"] == df["state_notification_rate"]) & (
            df["deaths"] <= 0
        )

        df["subnotification_place_type"] = np.where(mask, "state", "city")
        # print(df["subnotification_place_type"].value_counts()["city"])

    if place_id == "state":

        mask = df["notification_rate"] != np.nan
        df["subnotification_place_type"] = np.nan

    df["subnotification_rate"] = 1 - df["notification_rate"]

    # Ranking de subnotificação dos municípios para cada estado
    df["subnotification_rank"] = _get_subnotification_rank(df, mask, place_id)

    # Taxa de mortalidade até a ultima semana
    last_week = data["last_updated"].max() - dt.timedelta(7)

    df["last_mortality_ratio"] = (
        data[data["last_updated"] == last_week]
        .sort_values(place_id)
        .groupby(place_id)
        .sum()
        .apply(lambda row: _get_mortality_ratio(row), axis=1)
    )
    # Classificação: percentual de subnotificação
    df[classify] = _get_levels(df[mask], rules[classify])

    return df


def get_overall_alert(row, alerts):

    for alert, items in alerts.items():
        try:
            results = {
                col: row[col] in value for col, value in items["conditions"].items()
            }
            if items["how"] == "any":
                if any(results.values()):
                    return alert
            if items["how"] == "all":
                if all(results.values()):
                    return alert
        # Caso algum np.nan
        except TypeError:
            return np.nan


@allow_local
def now(config):

    # TODO: add to config on simulacovid
    config["farolcovid"] = yaml.load(
        open("endpoints/aux/farol_config.yaml"), Loader=yaml.FullLoader
    )

    df = (
        get_simulacovid_main.now(config)[config["farolcovid"]["simulacovid"]["columns"]]
        .sort_values("city_id")
        .set_index("city_id")
        .assign(confirmed_cases=lambda x: x["confirmed_cases"].fillna(0))
        .assign(deaths=lambda x: x["deaths"].fillna(0))
        .assign(
            number_beds=lambda x: config["farolcovid"]["resources"][
                "available_proportion"
            ]
            * x["number_beds"]
            / config["br"]["health"]["initial_proportion"]
        )
        .assign(
            number_ventilators=lambda x: config["farolcovid"]["resources"][
                "available_proportion"
            ]
            * x["number_ventilators"]
            / config["br"]["health"]["initial_proportion"]
        )
    )

    # Calcula indicadores, classificações e crescimento
    df = get_indicators_subnotification(
        df,
        data=get_cases.now(config).rename({"state": "state_id"}, axis=1),
        place_id="city_id",
        rules=config["farolcovid"]["rules"],
        classify="subnotification_classification",
    )

    df = get_indicators_rt(
        df,
        data=get_cities_rt.now(config),
        place_id="city_id",
        rules=config["farolcovid"]["rules"],
        classify="rt_classification",
        growth="rt_growth",
    )

    df = get_indicators_inloco(
        df,
        data=get_inloco_cities.now(config),
        place_id="city_id",
        rules=config["farolcovid"]["rules"],
        growth="inloco_growth",
    )

    df = get_indicators_capacity(
        df, config, rules=config["farolcovid"]["rules"], classify="dday_classification"
    )

    df["overall_alert"] = df.apply(
        lambda x: get_overall_alert(x, config["farolcovid"]["alerts"]), axis=1
    ).replace("medio2", "medio")

    return df.reset_index()


TESTS = {
    "more than 5570 cities": lambda df: len(df["city_id"].unique()) <= 5570,
    "df is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    # "city without subnotification rate got a rank": lambda df: df[
    #     "subnotification_place_type"
    # ].value_counts()["city"]
    # == df["subnotification_rank"].count(),
    "city doesnt have both rt classified and growth": lambda df: df[
        "rt_classification"
    ].count()
    == df["rt_growth"].count(),
    "dday worst greater than best": lambda df: len(
        df[df["dday_beds_worst"] > df["dday_beds_best"]]
    )
    == 0,
    "city with all classifications got null alert": lambda df: all(
        df[df["overall_alert"].isnull()][
            [
                "rt_classification",
                "rt_growth",
                "dday_classification",
                "subnotification_classification",
            ]
        ]
        .isnull()
        .apply(lambda x: any(x), axis=1)
        == True
    ),
    "city without deaths has mortality ratio": lambda df: len(
        df[(df["deaths"] == 0) & (~df["last_mortality_ratio"].isnull())]
    )
    == 0,
}
