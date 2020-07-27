import pandas as pd
from utils import treat_text, download_from_drive
from endpoints.helpers import allow_local
from numpy import dtype


@allow_local
def now(config, country="br"):
    updates = (
        download_from_drive(config[country]["drive_paths"]["CNAE_sectors"])
        .assign(cnae=lambda df: df["cnae"].astype("int64"))
        .assign(essential=lambda df: df["essential"].astype("bool"))
    )

    return updates


TESTS = {
    "data is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    "data without at least 74 sectors": lambda df: len(df) >= 74,
    "data contains null datafield": lambda df: df.isna().sum().sum() == 0,
    "CNAE field is not exclusively ints": lambda df: df["cnae"].dtype == dtype("int64"),
    "essential field is not exclusively bool": lambda df: df["essential"].dtype
    == dtype("bool"),
}

if __name__ == "__main__":

    pass
