# from endpoints import get_embaixadores
from endpoints import get_cases, get_health
import pandas as pd
import numpy as np
from copy import deepcopy

from utils import get_last
from endpoints.helpers import allow_local


def _recover_notification_rate(row, rates):
    """
    Recupera a taxa de notificação da regional para cidades sem casos i.e. que não vêm de get_cases
    """

    if np.isnan(row["health_region_notification_rate"]):
        return rates.loc[row["health_region_id"]].values[0]
    else:
        return row["health_region_notification_rate"]


@allow_local
def now(config):

    df = get_health.now(config, "br")[config["br"]["simulacovid"]["columns"]["cnes"]]

    # merge cases
    cases = get_cases.now(config, "br")
    cases = cases[cases["is_last"] == True].drop(config["br"]["cases"]["drop"], 1)

    df = df.merge(cases, on="city_id", how="left", suffixes=("", "_y"))
    df = df[[c for c in df.columns if not c.endswith("_y")]]

    health_region_rate = (
        df[["health_region_id", "health_region_notification_rate"]]
        .dropna()
        .groupby("health_region_id")
        .mean()
    )

    # get notification for cities without cases
    df["health_region_notification_rate"] = df.apply(
        lambda row: _recover_notification_rate(row, health_region_rate), axis=1
    )

    df["notification_rate"] = np.where(
        df["notification_rate"].isnull(),
        df["health_region_notification_rate"],
        df["notification_rate"],
    )

    df["last_updated"] = pd.to_datetime(df["last_updated"])
    return df


TESTS = {
    "len(data) != 5570": lambda df: len(df) == 5570,
    "data is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    "notification_rate == NaN": lambda df: len(
        df[df["notification_rate"].isnull() == True]
    )
    == 0,
    "no negative beds or icu beds": lambda df: len(
        df.query("number_beds < 0 | number_icu_beds < 0")
    )
    == 0,
}
