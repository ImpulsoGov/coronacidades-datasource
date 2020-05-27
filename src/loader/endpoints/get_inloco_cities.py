import pandas as pd
from utils import secrets, get_googledrive_df
from logger import logger
from endpoints.helpers import allow_local
import endpoints.corrections as cr
import os

configs_path = os.path.join(os.path.dirname(__file__), "aux")
cities_table = pd.read_csv(os.path.join(configs_path, "cities_table.csv"))
states_table = pd.read_csv(os.path.join(configs_path, "states_table.csv"))

# Adds the city_id column on inlocos data (must be after cleaning)
def insert_city_id(in_df):
    df = in_df
    ids = []
    for index, row in df.iterrows():
        state_num_id = states_table.loc[
            states_table["state_name"] == row["state_name"]
        ]["state_num_id"].values[0]
        try:
            city_id = cities_table.loc[
                (cities_table["state_num_id"] == state_num_id)
                & (cities_table["city_name"] == row["city_name"])
            ]["city_id"].values[0]
        except:
            print(
                states_table.loc[states_table["state_name"] == row["state_name"]][
                    "state_num_id"
                ].values
            )
            print(
                cities_table.loc[cities_table["city_name"] == row["city_name"]],
                row["city_name"],
                row["state_name"],
            )
        ids.append(city_id)
    df["city_id"] = ids
    return df


@allow_local
def now(config):

    return get_googledrive_df(secrets(["inloco", "cities", "id"]), 
            "token.pickle")


TESTS = {
    "data is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
}
