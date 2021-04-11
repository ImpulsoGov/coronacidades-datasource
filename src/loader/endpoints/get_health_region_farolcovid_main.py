import pandas as pd

from endpoints import get_health_region_cases, get_health_region_rt
from endpoints.get_indicators import get_place_indicators
from endpoints.helpers import allow_local


@allow_local
def now(config):

    # Get resource data
    data_cases = get_health_region_cases.now(config)
    data_rt = get_health_region_rt.now(config)

    return get_place_indicators(
        place_id="health_region_id",
        data_cases=data_cases,
        data_rt=data_rt,
        config=config,
    )


TESTS = {
    "doesnt have 27 states": lambda df: len(df["state_id"].unique()) == 27,
    "overall alert > 3": lambda df: all(
        df[~df["overall_alert"].isnull()]["overall_alert"] <= 3
    ),
    "doesnt have 450 regions": lambda df: len(
        df["health_region_id"].unique()
    ) == 450,
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
    ),
}
