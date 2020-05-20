import pandas as pd


def _get_rolling_amount(grp, time, data_col="last_updated", col_to_roll="deaths"):
    return grp.rolling(time, min_periods=1, on=data_col)[col_to_roll].mean()


def now(config):

    df = (
        pd.read_csv("https://covid.ourworldindata.org/data/owid-covid-data.csv")
        .dropna(subset=["new_deaths"])[
            ["iso_code", "date", "total_deaths", "new_deaths"]
        ]
        .groupby(["iso_code", "date", "total_deaths"])["new_deaths"]
        .sum()
        .reset_index()
    )

    df["rolling_deaths_new"] = df.groupby(
        "iso_code", as_index=False, group_keys=False
    ).apply(lambda x: _get_rolling_amount(x, 5, "date", "new_deaths"))

    return df[df["iso_code"] != "OWID_WRL"]


TESTS = {
    "data is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
}
