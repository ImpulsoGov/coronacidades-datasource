import pandas as pd
import sys
import numpy as np
from utils import secrets, get_googledrive_df, configs_path
from endpoints.helpers import allow_local
import Levenshtein as lev
import os
import yaml
from logger import logger


class StateFrame:
    """
    Helper class to get a dataframe without state and city numerical ids and insert them while also correcting their city names
    It works by putting most of the stuff in np arrays and storing stuff in sorted arrays to be abe to user binary search to find them quickly.
    """

    def __init__(
        self, df, in_cities_table, in_states_table, in_corrections_config
    ):  # df is inloco. This Function is the main of the cleaning process
        self.corrections = in_corrections_config
        self.cities_table = in_cities_table
        in_states_table = in_states_table.sort_values(by=["state_name"])
        self.states_names = in_states_table["state_name"].values
        self.states_num_ids = in_states_table["state_num_id"].values
        csid = np.vectorize(self.convert_state_name_to_num_id)
        df["state_num_id"] = csid(df["state_name"].values)

        # Separing the inloco cities in each sorted state
        self.states = [np.empty(0, dtype="<U32") for i in range(55)]
        sort_vectorized = np.vectorize(self.sort_df)
        sort_vectorized(df["state_num_id"].values, df["city_name"].values)
        # Separing the reference cities in each sorted state
        self.states_references = [np.empty([0, 2], dtype="<U32") for i in range(55)]
        sort_ref_vectorized = np.vectorize(self.sort_ref)
        sort_ref_vectorized(
            self.cities_table["state_num_id"].values,
            self.cities_table["city_name"].values,
            self.cities_table["city_id"].values,
        )
        # Finding the ones that do not match
        self.corrections_table = [np.empty(0, dtype="<U32") for i in range(55)]
        for state_num_id in self.states_num_ids:
            state_cities_ref = self.states_references[state_num_id]
            state_cities_inloco = self.states[state_num_id]
            self.correct_names(state_cities_inloco, state_cities_ref, state_num_id)
        final_df = df.copy(deep=True)
        final_df["city_name"], final_df["city_id"], final_df["state_num_id"] = zip(
            *final_df.apply(self.finalize_df, axis=1)
        )
        self.final_df = final_df

    def get_clean_df(self):
        return self.final_df

    def sort_df(self, state_num_id, city_name):
        idx = self.states[state_num_id].searchsorted(city_name)
        if (
            idx >= len(self.states[state_num_id])
            or self.states[state_num_id][idx] != city_name
        ):
            self.states[state_num_id] = np.concatenate(
                (
                    self.states[state_num_id][:idx],
                    [city_name],
                    self.states[state_num_id][idx:],
                )
            )

    def sort_ref(self, state_num_id, city_name, city_id):
        idx = self.states_references[state_num_id][:, 0].searchsorted(
            city_name
        )  # uses only the first item
        if (
            idx >= len(self.states_references[state_num_id])
            or self.states_references[state_num_id][idx][0] != city_name
        ):
            self.states_references[state_num_id] = np.concatenate(
                (
                    self.states_references[state_num_id][:idx],
                    np.array([[city_name, str(city_id)]]),
                    self.states_references[state_num_id][idx:],
                )
            )

    def convert_state_name_to_num_id(self, state_name):
        state_index = np.searchsorted(self.states_names, state_name)
        return self.states_num_ids[state_index]

    def solve_word(self, wrong_word, correct_word, lev_dis):
        if lev_dis <= 2:
            return correct_word
        else:
            try:
                return self.corrections["inloco"][wrong_word]["correct_name"]
            except:
                logger.warning("City not found in corrections: " + wrong_word)

    def minlev2(self, wrong_name, correct_state):
        min_dis = float("inf")
        word = None
        for correct_city in correct_state:
            dis = lev.distance(wrong_name, correct_city)
            if dis < min_dis:
                word = correct_city
                min_dis = dis
        return (min_dis, word)

    def correct_names(self, wrong_state_cities, correct_state, state_id):
        correct_state_cities = correct_state[:, 0]
        not_found = np.where(
            np.isin(wrong_state_cities, correct_state_cities), None, wrong_state_cities
        )
        not_found = not_found[not_found != np.array(None)]
        corrections_state = np.empty([0, 3], dtype="<U32")
        for not_found_name in not_found:
            min_dis, closest = self.minlev2(not_found_name, correct_state_cities)
            final_name = self.solve_word(not_found_name, closest, min_dis)
            final_name_index = np.searchsorted(correct_state_cities, final_name)
            final_name_id = correct_state[final_name_index][1]
            correction_data = np.array([not_found_name, final_name, str(final_name_id)])
            idx = np.searchsorted(corrections_state[:, 0], correction_data[0])
            corrections_state = np.concatenate(
                (corrections_state[:idx], [correction_data], corrections_state[idx:])
            )
        self.corrections_table[state_id] = corrections_state
        return corrections_state

    def finalize_df(self, row):
        state_id = self.convert_state_name_to_num_id(row["state_name"])
        corrections = self.corrections_table[state_id]
        correction_index = np.searchsorted(corrections[:, 0], row["city_name"])
        # If is to be corrected
        if (
            correction_index < len(corrections)
            and corrections[correction_index][0] == row["city_name"]
        ):
            final_name = corrections[correction_index][1]
            final_id = int(corrections[correction_index][2])
        else:  # Just get the city id
            final_name = row["city_name"]
            index = self.states_references[state_id][:, 0].searchsorted(
                row["city_name"]
            )
            final_id = self.states_references[state_id][index][1]
        return final_name, final_id, state_id


@allow_local
def now(config):
    file_id = secrets(["inloco", "cities", "id"])
    cities_table = pd.read_csv(os.path.join(configs_path, "cities_table.csv"))
    states_table = pd.read_csv(os.path.join(configs_path, "states_table.csv"))
    corrections_table = yaml.load(
        open(os.path.join(configs_path, "city_corrections.yaml"), "r"),
        Loader=yaml.FullLoader,
    )
    raw_inloco_cities = get_googledrive_df(file_id)
    return StateFrame(
        raw_inloco_cities, cities_table, states_table, corrections_table
    ).get_clean_df()


TESTS = {
    "data is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
}
