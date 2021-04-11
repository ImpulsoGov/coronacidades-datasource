import pandas as pd
import numpy as np

from endpoints import (
    get_states_beds_occupation,
    get_cnes,
)
from logger import logger


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
    """Calculate how many days the group is on the last growth status"""
    group = group.reset_index()[
        [place_id, "daily_cases_growth"]
    ].drop_duplicates(keep="last")
    # if only had one status since the beginning
    if len(group) == 1:
        group["daily_cases_growth_ndays"] = group.index[-1]
    else:
        group["daily_cases_growth_ndays"] = group.index[-1] - group.index[-2]

    return group.tail(1)


def get_situation_indicators(df, data, place_id, rules, classify):
    logger.info("Consolidating situation indicators on '{}'".format(place_id))
    data["last_updated"] = pd.to_datetime(data["last_updated"])

    # Get ndays of last growth status
    df["daily_cases_growth_ndays"] = (
        data.sort_values(by=[place_id, "last_updated"])
        .groupby(place_id)
        .apply(lambda group: _get_growth_ndays(group, place_id))
        .reset_index(drop=True)
        .set_index(place_id)
    )["daily_cases_growth_ndays"]

    data = data.loc[data.groupby(place_id)["last_updated"].idxmax()].set_index(
        place_id
    )
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
    df, data, place_id, rules, classify, region_data=None,
):
    logger.info("Consolidating control indicators on '{}'".format(place_id))

    rename = {
        i: i.lower()
        for i in [
            "Rt_low_95",
            "Rt_high_95",
            "Rt_most_likely",
            "Rt_most_likely_growth",
        ]
    }
    rename["last_updated"] = "last_updated_rt"

    data = data.astype({"last_updated": "datetime64"}).rename(columns=rename)

    # Min-max do Rt de 14 dias (max data de taxa de notificacao) -> 10 dias
    # atrás (KEVIN & COVIDACTNOW)
    data = data.loc[data.groupby(place_id)["last_updated_rt"].idxmax(), :]

    df = (
        df
        .merge(data, suffixes=("", "_drop"), on=place_id, how="left")
        .reset_index()
    )
    df = df.drop(columns=[col for col in df.columns if "_drop" in col])

    # Completa com Rt da regional
    if place_id == "city_id":

        region_data = (
            region_data
            .astype({"last_updated": "datetime64"})
            .rename(columns=rename)
        )
        region_data = region_data.loc[
            region_data
            .groupby("health_region_id")["last_updated_rt"]
            .idxmax(),
            :
        ]

        df["rt_place_type"] = (
            df["rt_most_likely"]
            .isnull()
            .map({True: "health_region_id", False: "city_id"})
        )
        df_city_rt = df.loc[df["rt_place_type"] == "city_id", :]
        df_region_rt = df.loc[df["rt_place_type"] == "health_region_id", :]

        df_city_rt = (
            df_city_rt
            .merge(
                region_data,
                suffixes=("_drop", ""),
                on="health_region_id",
                how="left",
            )
            .reset_index()
        )
        df_city_rt = df_city_rt.drop(
            columns=[col for col in df_city_rt.columns if "_drop" in col]
        )

        df = pd.concat([df_city_rt, df_region_rt], ignore_index=True)

    # Classificação: melhor estimativa do Rt de 10 dias (rt_most_likely)
    df[classify] = _get_levels(df, rules[classify])

    return df


# TRUST
# TODO: add here after update on cases df
def get_trust_indicators(df, data, place_id, rules, classify):
    logger.info("Consolidating trust indicators on '{}'".format(place_id))

    data["last_updated"] = pd.to_datetime(data["last_updated"])

    # Última data com notificação: 14 dias atrás
    df[
        ["last_updated_subnotification", "notification_rate", "active_cases"]
    ] = (
        data.dropna()
        .groupby(place_id)[
            ["last_updated", "notification_rate", "active_cases"]
        ]
        .last()
    )

    df["subnotification_rate"] = 1 - df["notification_rate"]

    # Classificação: percentual de subnotificação
    df[classify] = _get_levels(df, rules[classify])

    return df


def get_capacity_indicators(df, config, rules, classify):
    logger.info("Consolidating capacity indicators")

    # agregar leitos por estado (não faz diferença se `df` já for por estado)
    capacity_state = (
        df.groupby(
            [
                "state_num_id",
                "last_updated_number_beds",
                "author_number_beds",
                "last_updated_number_icu_beds",
                "author_number_icu_beds",
            ]
        )[["population", "number_beds", "number_icu_beds"]]
        .sum()
        .reset_index()
    )

    # Leitos UTI e enfermaria por 100k habitantes
    df["number_beds_100k"] = (10 ** 5) * (df["number_beds"] / df["population"])
    df["number_icu_beds_100k"] = (10 ** 5) * (
        df["number_icu_beds"] / df["population"]
    )

    # ocupação em UTIs no estado
    occupation = (
        get_states_beds_occupation
        .now(config)
        .astype({"last_updated": "datetime64"})
        .rename(columns={"last_updated": "last_updated_icu_occupation"})
    )
    occupation_latest = occupation.loc[
        occupation
        .groupby("state_num_id")["last_updated_icu_occupation"]
        .idxmax(),
        :
    ]
    capacity_state = capacity_state.merge(
        occupation_latest, on="state_num_id", suffixes=("", "_drop")
    ).reset_index()

    # Juntar ao DataFrame original
    df = df.merge(
        capacity_state, on="state_num_id", suffixes=("", "_drop")
    )

    # remove colunas duplicadas
    df = df.drop(columns=[col for col in df if "_drop" in col])

    df[classify] = _get_levels(df, rules[classify])

    return df


def get_overall_alert(indicators):
    if indicators.notnull().all():
        return int(max(indicators))
    else:
        return np.nan


def get_place_indicators(
    place_id, data_cases, data_rt, config, data_rt_region=None
):
    cnes_cols = [
        "country_iso",
        "country_name",
        "state_num_id",
        "state_id",
        "state_name",
        "last_updated_number_beds",
        "author_number_beds",
        "last_updated_number_icu_beds",
        "author_number_icu_beds",
    ]
    if place_id == "health_region_id" or place_id == "city_id":
        cnes_cols += ["health_region_id", "health_region_name"]
    if place_id == "city_id":
        cnes_cols += ["city_id", "city_name"]

    rules = config["br"]["farolcovid"]["rules"]

    df = (
        get_cnes.now(config)
        .groupby(cnes_cols)
        .agg({"population": sum, "number_beds": sum, "number_icu_beds": sum})
        .reset_index()
        .set_index(place_id)
        .sort_index()
        .pipe(
            get_situation_indicators,
            data=data_cases,
            place_id=place_id,
            rules=rules,
            classify="situation_classification",
        )
        .pipe(
            get_control_indicators,
            data=data_rt,
            place_id=place_id,
            rules=rules,
            classify="control_classification",
            region_data=data_rt_region,
        )
        .pipe(
            get_trust_indicators,
            data=data_cases,
            place_id=place_id,
            rules=rules,
            classify="trust_classification",
        )
        .pipe(
            get_capacity_indicators,
            rules=rules,
            classify="capacity_classification",
            config=config,
        )
    )

    classif_cols = [col for col in df.columns if "classification" in col]
    df["overall_alert"] = df.apply(
        lambda row: get_overall_alert(row[classif_cols]), axis=1
    )

    return df
