import pandas as pd
from utils import treat_text, download_from_drive
from endpoints.helpers import allow_local
from numpy import dtype


@allow_local
def now(config, country="br"):
    updates = download_from_drive(config[country]["drive_paths"]["CNAE_sectors"])
    # updates = download_from_drive("https://docs.google.com/spreadsheets/d/1pP8dY9kRa9EvX3KcVPfMWJ8exDaoT1_bBggSKfD3y0k")
    return updates


TESTS = {
    "data is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    "at least 74 sectors": lambda df: len(df) >= 74,
    "is essential field contains null": lambda df: df["essential"].isna().sum() == 0,
    "CNAE field is not exclusively ints": lambda df: df["cnae"].dtype == dtype("int64"),
    "is essential field is not exclusively bools": lambda df: df["essential"].dtype
    == dtype("bool"),
}

if __name__ == "__main__":

    pass
