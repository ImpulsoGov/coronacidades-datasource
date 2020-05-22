import pandas as pd
import numpy as np
import yaml
from scipy.integrate import odeint
from tqdm import tqdm
import sys

from endpoints.aux.seir import entrypoint
import datetime as dt


def get_dday(dfs, col, resource_number):

    dday = dict()
    for case in ["worst", "best"]:
        df = dfs[case]

        if max(df[col]) > resource_number:
            dday[case] = df[df[col] > resource_number].index[0]
        else:
            dday[case] = -1  # change here!

    return dday


def run_simulation(user_input, config):

    dfs = {"worst": np.nan, "best": np.nan}

    # Run worst scenario
    for bound in dfs.keys():

        # Run model projection
        res = entrypoint(
            user_input["population_params"],
            config["br"]["seir_parameters"],
            phase={
                "scenario": "projection_current_rt",
                "R0": user_input["R0"][bound],
                "n_days": 90,
            },
            initial=True,
        )

        res = res.reset_index(drop=True)
        res.index += 1
        res.index.name = "dias"

        dfs[bound] = res

    dday_beds = get_dday(dfs, "I2", user_input["n_beds"])
    dday_ventilators = get_dday(dfs, "I3", user_input["n_ventilators"])

    return dday_beds, dday_ventilators


if __name__ == "__main__":
    pass
