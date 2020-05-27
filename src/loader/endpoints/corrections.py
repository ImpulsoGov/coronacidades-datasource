import pandas as pd
import Levenshtein as lev

main_corrections = pd.DataFrame(
    {
        "city_name": ["Januário Cicco"],
        "state_num_id": [24],
        "correction_name": ["Boa Saúde"],
    }
)
in_loco_corrections = pd.DataFrame(
    {
        "city_name": ["São Valério da Natividade", "Santa Teresinha", "Campo Grande"],
        "state_name": ["Tocantins", "Bahia", "Rio Grande do Norte"],
        "correction_name": ["São Valério", "Santa Terezinha", "Augusto Severo"],
    }
)

# In: Name of the city, id of its state and the database over which to compare it
# Out: the minimal Lev distance found and the closes word for it (within the same state always)
def min_lev(city, state_id, target_df):
    min_dis = float("inf")
    word = None
    for index, row in target_df.iterrows():
        if state_id == row["state_num_id"]:  # if on the same state
            dis = lev.distance(city, row["city_name"])
            if dis < min_dis:
                word = row["city_name"]
                min_dis = dis
    return (min_dis, word)


# IN: cities dataframe from main
# OUT cleaned version of it
def correct_main_cities(cities_df):
    cities_df_main = cities_df
    for index, correction in main_corrections.iterrows():
        cities_df_main.loc[
            (cities_df_main["state_num_id"] == correction["state_num_id"])
            & (cities_df_main["city_name"] == correction["city_name"]),
            "city_name",
        ] = correction["correction_name"]
    return cities_df_main


# In: The dataframe with mistakes, the reference of correct cities and the reference of correct states
# Out: A DataFrame will all the words that could not be matched from the dataframe to the correct reference
def get_not_found(cities_data, cities_table, states_table):
    new_cities_table = cities_table
    states_name = []
    for index, row in new_cities_table.iterrows():
        state_name = states_table.loc[
            states_table["state_num_id"] == row["state_num_id"]
        ]["state_name"].values[0]
        states_name.append(state_name)
    new_cities_table["state_name"] = states_name
    new_cities_df = cities_data[["state_name", "city_name"]].drop_duplicates(
        ignore_index=True
    )
    final_cities_df = new_cities_table[["state_name", "city_name"]]
    not_matched = pd.DataFrame()
    for index, new_city in new_cities_df.iterrows():
        matching_state_main = final_cities_df[
            final_cities_df["state_name"] == new_city["state_name"]
        ]
        matching_city_main = matching_state_main[
            matching_state_main["city_name"] == new_city["city_name"]
        ]
        if len(matching_city_main) == 0:  # no match
            not_matched = not_matched.append(new_city)
    return not_matched.reset_index(drop=True)


# IN: Inloco social distancing data, the citites_table.csv config dataframe, the states_table.csv dataframe
# OUT: The cleaned version of Inloco's dataframe
def correct_inloco_cities(cities_data, cities_table, states_table):
    inloco_cities_data = cities_data
    not_found_df = get_not_found(cities_data, cities_table, states_table)
    # PUTS state_num_id
    not_found_df["state_num_id"] = [
        states_table.loc[states_table["state_name"] == row["state_name"]][
            "state_num_id"
        ].values[0]
        for index, row in not_found_df.iterrows()
    ]
    for index, row in not_found_df.iterrows():  # city_name,state_name
        # correct_state_name = states_table.loc[states_table["state_num_id"] == row["state_num_id"]]["state_name"].values[0]  # gets from the states the correct state name
        min_dis, correct_word = min_lev(
            row["city_name"], row["state_num_id"], cities_table
        )
        if min_dis <= 2:  # too close to a city in main
            inloco_cities_data.loc[
                (inloco_cities_data["state_name"] == row["state_name"])
                & (inloco_cities_data["city_name"] == row["city_name"]),
                "city_name",
            ] = correct_word  # Changes the name of the city
    for index, correction in in_loco_corrections.iterrows():  # For the extreme cases
        inloco_cities_data.loc[
            (inloco_cities_data["state_name"] == correction["state_name"])
            & (inloco_cities_data["city_name"] == correction["city_name"]),
            "city_name",
        ] = correction["correction_name"]
    return inloco_cities_data
