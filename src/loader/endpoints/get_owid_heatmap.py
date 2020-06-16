import pandas as pd
from utils import get_country_isocode_name
from endpoints.helpers import allow_local


def _get_rolling_amount(grp, time, data_col="last_updated", col_to_roll="deaths"):
    return grp.rolling(time, min_periods=1, on=data_col)[col_to_roll].mean()


@allow_local
def now(config):

    df = (
        pd.read_csv(config["br"]["drive_paths"]["owid"])
        .dropna(subset=["new_deaths"])[
            ["iso_code", "date", "total_deaths", "new_deaths"]
        ]
        .groupby(["iso_code", "date", "total_deaths"])["new_deaths"]
        .sum()
        .reset_index()
        .assign(country_pt=lambda df: get_country_isocode_name(df["iso_code"]))
        .assign(new_deaths=lambda df: df["new_deaths"].clip(0))
        .assign(total_deaths=lambda df: df["total_deaths"].clip(0))
        .dropna(subset=["country_pt"])
    )

    df["rolling_deaths_new"] = df.groupby(
        "iso_code", as_index=False, group_keys=False
    ).apply(lambda x: _get_rolling_amount(x, 5, "date", "new_deaths"))

    return df


TESTS = {
    "data is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    "dataframe has null data": lambda df: all(df.isnull().any() == False),
    "total deaths is negative": lambda df: len(df.query("total_deaths < 0")) == 0,
    "new deaths is negative": lambda df: len(df.query("new_deaths < 0")) == 0,
    "rolling deaths is negative": lambda df: len(df.query("rolling_deaths_new < 0"))
    == 0,
}
