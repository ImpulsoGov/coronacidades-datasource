from endpoints import get_embaixadores
from endpoints import get_cases
from endpoints import get_health
import pandas as pd
import numpy as np
from copy import deepcopy

from utils import get_last

def _get_supplies(cities, updates, country, config):

    final_cols = config[country]["columns"]["final"]

    final = []
    for h in config[country]["columns"]["health"]:
        u = updates.rename(columns={"name": "author"})[final_cols + [h]].dropna(
            subset=[h]
        )
        u = get_last(u)
        cities["author"] = config[country]["health"]["source"]
        c = cities.rename(columns={"last_updated_" + h: "last_updated"})[
            final_cols + [h]
        ]
        c[h] = c[h] * config[country]["health"]["initial_proportion"]
        f = get_last(pd.concat([c, u]))
        f.columns = (
            ["city_id"] + [i + "_" + h for i in final_cols if i != "city_id"] + [h]
        )
        final.append(deepcopy(f))

    supplies = pd.concat(final, 1)
    supplies = supplies.loc[:, ~supplies.columns.duplicated()]

    return supplies


def now(config, last=True):

    # get health & population data
    updates = get_embaixadores.now(config, "br")
    cities = get_health.now(config, "br")

    # add ambassadors updates
    updates = cities[["state_id", "city_norm", "city_id"]].merge(
        updates, on=["state_id", "city_norm"], how="right"
    )

    supplies = _get_supplies(cities, updates, "br", config)

    # merge cities & supplies
    df = cities[
        [
            "country_iso",
            "country_name",
            "state_id",
            "state_name",
            "city_id",
            "city_name",
            "population",
            "health_system_region",
        ]
    ].merge(supplies, on="city_id")

    # merge cases
    cases = get_cases.now(config, "br", last=True)
    df = df.merge(cases, on="city_id", how="left")

    # get notification for cities without cases
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
    "notification_rate == NaN": lambda df: len(df["notification_rate"].isnull() == True) == 0,
}
