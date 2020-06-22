from endpoints import get_embaixadores
from endpoints import get_cases
from endpoints import get_health
import pandas as pd
import numpy as np
from copy import deepcopy

from utils import get_last
from endpoints.helpers import allow_local


def _fix_state_notification(row, states_rate):

    if np.isnan(row["state_notification_rate"]):
        return states_rate.loc[row["state_id"]].values[0]
    else:
        return row["state_notification_rate"]


@allow_local
def now(config):

    df = get_health.now(config, "br")[config["br"]["simulacovid"]["columns"]["cnes"]]

    # merge cases
    cases = get_cases.now(config, "br")
    cases = cases[cases["is_last"] == True].drop(config["br"]["cases"]["drop"], 1)

    df = df.merge(cases, on="city_id", how="left")

    states_rate = (
        df[["state_id", "state_notification_rate"]].dropna().groupby("state_id").mean()
    )

    # get notification for cities without cases
    df["state_notification_rate"] = df.apply(
        lambda row: _fix_state_notification(row, states_rate), axis=1
    )

    df["notification_rate"] = np.where(
        df["notification_rate"].isnull(),
        df["state_notification_rate"],
        df["notification_rate"],
    )

    df["last_updated"] = pd.to_datetime(df["last_updated"])

    return df


TESTS = {
    "len(data) != 5570": lambda df: len(df) == 5570,
    "data is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    "notification_rate == NaN": lambda df: len(
        df[df["notification_rate"].isnull() == True]) == 0,
    "more ventilators than beds": lambda df: len(df[df['number_ventilators'].values > df['number_beds'].values]) == 0
}
