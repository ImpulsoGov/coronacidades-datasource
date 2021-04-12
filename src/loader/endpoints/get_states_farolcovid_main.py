import pandas as pd

from endpoints import get_states_cases, get_states_rt
from endpoints.get_indicators import get_place_indicators
from endpoints.helpers import allow_local


@allow_local
def now(config):

    # Get resource data
    data_cases = get_states_cases.now(config)
    data_rt = get_states_rt.now(config)

    return get_place_indicators(
        place_id="state_num_id",
        data_cases=data_cases,
        data_rt=data_rt,
        config=config,
    )


TESTS = {
    "doesnt have 27 states": lambda df: len(df["state_num_id"].unique()) == 27,
    "df is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    "overall alert > 3": lambda df: all(
        df[~df["overall_alert"].isnull()]["overall_alert"] <= 3
    ),
    # "dataframe has null data": lambda df: all(df.isnull().any() == False),
    "doesnt have both rt classified and growth": lambda df: df[
        "control_classification"
    ].count()
    == df["rt_most_likely_growth"].count(),
    "rt 10 days maximum and minimum values": lambda df: all(
        df[
            ~(
                (df["rt_low_95"] < df["rt_most_likely"])
                & (df["rt_most_likely"] < df["rt_high_95"])
            )
        ]["rt_most_likely"].isnull()
    ),
    "state with all classifications got null alert": lambda df: all(
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
    ),
    # "state without classification got an alert": lambda df: all(
    #     df[
    #         df[
    #             [
    #                 "capacity_classification",
    #                 "control_classification",
    #                 "situation_classification",
    #                 "trust_classification",
    #             ]
    #         ]
    #         .isnull()
    #         .any(axis=1)
    #     ]["overall_alert"].isnull()
    #     == True
    # ),
}
