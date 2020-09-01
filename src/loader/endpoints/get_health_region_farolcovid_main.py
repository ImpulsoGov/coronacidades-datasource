import pandas as pd
import numpy as np
import datetime as dt
import yaml
from pathlib import Path

from endpoints import (
    get_health_region_cases,
    get_health_region_rt,
    get_states_rt,
    get_health,
)

from endpoints.helpers import allow_local
from endpoints.scripts.simulator import run_simulation


def _get_levels(df, rules):
    return pd.cut(
        df[rules["column_name"]],
        bins=rules["cuts"],
        labels=rules["categories"],
        right=False,
        include_lowest=True,
    )


# SITUATION: New cases
def _get_growth_ndays(group, place_id):
    """Calculate how many days the group is on the last growth status
    """
    group = group.reset_index()[[place_id, "daily_cases_growth"]].drop_duplicates(
        keep="last"
    )
    # if only had one status since the beginning
    if len(group) == 1:
        group["daily_cases_growth_ndays"] = group.index[-1]
    else:
        group["daily_cases_growth_ndays"] = group.index[-1] - group.index[-2]

    return group.tail(1)


def get_situation_indicators(df, data, place_id, rules, classify):

    data["last_updated"] = pd.to_datetime(data["last_updated"])

    # Get ndays of last growth status
    df["daily_cases_growth_ndays"] = (
        data.sort_values(by=[place_id, "last_updated"])
        .groupby(place_id)
        .apply(lambda group: _get_growth_ndays(group, place_id))
        .reset_index(drop=True)
        .set_index(place_id)
    )["daily_cases_growth_ndays"]

    data = data.loc[data.groupby(place_id)["last_updated"].idxmax()].set_index(place_id)
    df["last_updated_cases"] = data["last_updated"]

    # Get indicators & update cases and deaths to current date
    cols = [
        "confirmed_cases",
        "daily_cases",
        "deaths",
        "new_deaths",
        "daily_cases_mavg_100k",
        "daily_cases_growth",
        "new_deaths_mavg_100k",
        "new_deaths_growth",
    ]
    df[cols] = data[cols]

    df[classify] = _get_levels(df, rules[classify])
    df[classify] = df.apply(
        lambda row: row[classify] + 1
        if (row["daily_cases_growth"] == "crescendo" and row[classify] < 3)
        else row[classify],
        axis=1,
    )

    return df


# CONTROL: - (no testing data!)
def get_control_indicators(
    df, data, place_id, rules, classify, config=None, region_data=None
):

    data = data.assign(last_updated=lambda df: pd.to_datetime(df["last_updated"]))
    data = data.loc[data.groupby(place_id)["last_updated"].idxmax()]

    # Min-max do Rt de 14 dias (max data de taxa de notificacao)
    df[
        [
            "last_updated_rt",
            "rt_low_95",
            "rt_high_95",
            "rt_most_likely",
            "rt_most_likely_growth",
        ]
    ] = data.sort_values(place_id).set_index(place_id)[
        [
            "last_updated",
            "Rt_low_95",
            "Rt_high_95",
            "Rt_most_likely",
            "Rt_most_likely_growth",
        ]
    ]

    # Completa com Rt da regional
    if place_id == "city_id":

        df["rt_place_type"] = (
            df["rt_most_likely"]
            .isnull()
            .map({True: "health_region_id", False: "city_id"})
        )

        cols = [
            "last_updated_rt",
            "health_region_id",
            "rt_low_95",
            "rt_high_95",
            "rt_most_likely",
            "rt_most_likely_growth",
        ]

        data = region_data.assign(
            last_updated=lambda df: pd.to_datetime(df["last_updated"])
        )

        data = data.loc[data.groupby("health_region_id")["last_updated"].idxmax()][
            [
                "last_updated",
                "health_region_id",
                "Rt_low_95",
                "Rt_high_95",
                "Rt_most_likely",
                "Rt_most_likely_growth",
            ]
        ]
        data.columns = cols

        # add city_id
        data = (
            df[["health_region_id"]]
            .reset_index()
            .merge(data, on="health_region_id")
            .set_index("city_id")
        )
        df.loc[:, cols] = df[cols].fillna(data)

    # Classificação: melhor estimativa do Rt de 10 dias (rt_most_likely)
    df[classify] = _get_levels(df, rules[classify])

    return df


# CAPACITY
def _calculate_recovered(df, params):

    confirmed_adjusted = int(df[["confirmed_cases"]].sum() / df["notification_rate"])

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


def _prepare_simulation(row, place_id, config, hospitalization_params, rt_upper=None):

    # based on Alison Hill: 30% asymptomatic
    symtomatic = [
        int(row["active_cases"] * 0.7) if not np.isnan(row["active_cases"]) else 1
    ][0]

    params = {
        "population_params": {
            "N": int(row["population"]),
            "I": symtomatic,
            "D": [int(row["deaths"]) if not np.isnan(row["deaths"]) else 0][0],
        },
        "hospitalization_params": {
            "i2_percentage": hospitalization_params["i2_percentage"].loc[int(row.name)],
            "i3_percentage": hospitalization_params["i3_percentage"].loc[int(row.name)],
        },
        "n_beds": row["number_beds"]
        * config["br"]["simulacovid"]["resources_available_proportion"],
        "n_icu_beds": row["number_icu_beds"],
        "R0": {
            "best": row["rt_most_likely"],  # só usamos o "best" neste caso
            "worst": row["rt_high_95"],
        },
    }

    # TODO: checar esses casos no calculo da subnotificacao!
    if row["notification_rate"] != row["notification_rate"]:
        return np.nan

    if row["notification_rate"] == 0:
        return np.nan

    # TODO: precisa? Seleciona rt de 1 nivel acima caso não tenha
    if row["rt_most_likely"] != row["rt_most_likely"]:
        if place_id == "health_region_id":
            rt = rt_upper.query(f"state_num_id == {row['state_num_id']}")
        else:
            return np.nan

        if len(rt) > 0:
            rt = rt.assign(
                last_updated=lambda df: pd.to_datetime(df["last_updated"])
            ).query("last_updated == last_updated.max()")
            params["R0"] = {"best": rt["Rt_most_likely"], "worst": rt["Rt_high_95"]}
        else:
            return np.nan

    params = _calculate_recovered(row, params)
    _, dday_icu_beds = run_simulation(params, config)

    return dday_icu_beds["best"]


def _get_infectious_hospitalized(place_id, config):
    # Get stratified pop data
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

    # Get total perc of hospitalized weighted by age
    df = pd.DataFrame(
        (
            pop.drop(columns=[col for col in pop.columns if "_id" in col])
            .apply(lambda row: row / row["total"], axis=1)
            .drop(columns=["total"])
            .dot(pd.Series(config["br"]["seir_parameters"]["hospitalized_by_age_perc"]))
        ),
        columns=["hospitalized_by_age_perc"],
    )

    i3_perc = config["br"]["seir_parameters"]["i3_percentage"]
    i2_perc = config["br"]["seir_parameters"]["i2_percentage"]

    # Get total perc of I2 (severe) & I3 (critical) hospitalized weighted by age
    df["i2_percentage"] = i2_perc * df["hospitalized_by_age_perc"] / (i2_perc + i3_perc)
    df["i3_percentage"] = i3_perc * df["hospitalized_by_age_perc"] / (i2_perc + i3_perc)

    return df


def get_capacity_indicators(df, place_id, config, rules, classify, data=None):

    if place_id == "health_region_id":
        rt_upper = get_states_rt.now(config)

    if place_id == "state_num_id":
        rt_upper = None

    # Pega valores calculados para regional e soma total de leitos
    if place_id == "city_id":
        df = (
            df.reset_index()
            .merge(
                data[
                    [
                        "dday_icu_beds",
                        "number_beds",
                        "number_icu_beds",
                        "health_region_id",
                    ]
                ],
                on="health_region_id",
                suffixes=("_drop", ""),
            )
            .set_index("city_id")
        )

        df[classify] = _get_levels(df, rules[classify])
        return df.drop(columns=[col for col in df if "_drop" in col])

    else:
        hospitalization_params = _get_infectious_hospitalized(place_id, config)
        df["dday_icu_beds"] = df.apply(
            lambda row: _prepare_simulation(
                row, place_id, config, hospitalization_params, rt_upper
            ),
            axis=1,
        )

    df["dday_icu_beds"] = df["dday_icu_beds"].replace(-1, 91)

    # Classificação: numero de dias para acabar a capacidade
    df[classify] = _get_levels(df, rules[classify])

    return df


# TRUST
# TODO: add here after update on cases df
def get_trust_indicators(df, data, place_id, rules, classify):

    data["last_updated"] = pd.to_datetime(data["last_updated"])

    # Última data com notificação: 14 dias atrás
    df[["last_updated_subnotification", "notification_rate", "active_cases"]] = (
        data.dropna()
        .groupby(place_id)[["last_updated", "notification_rate", "active_cases"]]
        .last()
    )

    df["subnotification_rate"] = 1 - df["notification_rate"]

    # Classificação: percentual de subnotificação
    df[classify] = _get_levels(df, rules[classify])

    return df


def get_overall_alert(indicators):
    if indicators.notnull().all():
        return int(max(indicators))
    else:
        return np.nan


@allow_local
def now(config):

    # Get resource data
    df = (
        get_health.now(config, "br")
        .groupby(
            [
                "country_iso",
                "country_name",
                "state_num_id",
                "state_id",
                "state_name",
                "health_region_id",
                "health_region_name",
                "last_updated_number_beds",
                "author_number_beds",
                "last_updated_number_icu_beds",
                "author_number_icu_beds",
            ]
        )
        .agg({"population": sum, "number_beds": sum, "number_icu_beds": sum})
        .reset_index()
        .sort_values("health_region_id")
        .set_index("health_region_id")
    )

    # TODO: get_cases => get_states_cases / mudar indicadores de situacao + add trust (notification_rate)!
    df = get_situation_indicators(
        df,
        data=get_health_region_cases.now(config),
        place_id="health_region_id",
        rules=config["br"]["farolcovid"]["rules"],
        classify="situation_classification",
    )

    df = get_control_indicators(
        df,
        data=get_health_region_rt.now(config),
        place_id="health_region_id",
        rules=config["br"]["farolcovid"]["rules"],
        classify="control_classification",
    )

    df = get_trust_indicators(
        df,
        data=get_health_region_cases.now(config),
        place_id="health_region_id",
        rules=config["br"]["farolcovid"]["rules"],
        classify="trust_classification",
    )

    df = get_capacity_indicators(
        df,
        place_id="health_region_id",
        config=config,
        rules=config["br"]["farolcovid"]["rules"],
        classify="capacity_classification",
    )

    cols = [col for col in df.columns if "classification" in col]
    df["overall_alert"] = df.apply(
        lambda row: get_overall_alert(row[cols]), axis=1
    )  # .replace(config["br"]["farolcovid"]["categories"])

    return df.reset_index()


TESTS = {
    "doesnt have 27 states": lambda df: len(df["state_id"].unique()) == 27,
    "overall alert > 3": lambda df: all(
        df[~df["overall_alert"].isnull()]["overall_alert"] <= 3
    ),
    "doesnt have 450 regions": lambda df: len(df["health_region_id"].unique()) == 450,
    "df is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    # "dataframe has null data": lambda df: all(df.isnull().any() == False),
    "doesnt have both rt classified and growth": lambda df: df[
        "control_classification"
    ].count()
    == df["rt_most_likely_growth"].count(),
    "region with all classifications got null alert": lambda df: all(
        df[df["overall_alert"].isnull()][
            [
                "control_classification",
                "situation_classification",
                "capacity_classification",
                "trust_classification",
            ]
        ]
        .isnull()
        .apply(lambda x: any(x), axis=1)
        == True
    ),
    "region without classification got an alert": lambda df: all(
        df[
            df[
                [
                    "capacity_classification",
                    "control_classification",
                    "situation_classification",
                    "trust_classification",
                ]
            ]
            .isnull()
            .any(axis=1)
        ]["overall_alert"].isnull()
        == True
    ),
}
