import pandas as pd
from utils import treat_text, download_from_drive
from endpoints.helpers import allow_local
from numpy import dtype


@allow_local
def now(config, country="br"):
    updates = (
        download_from_drive(config[country]["drive_paths"]["reopening_data"])
        .rename(config["br"]["safereopen"]["rename"], axis=1)
        .assign(state_id=lambda df: df["state_id"].astype("int64"))
        .assign(cnae=lambda df: df["cnae"].astype("int64"))
    )

    return updates


TESTS = {
    "data is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    "data contains null datafield": lambda df: df.isnull().sum().sum() == 0,
    "CNAE field is not exclusively ints": lambda df: df["cnae"].dtype == dtype("int64"),
    "UF field is not exclusively ints": lambda df: df["state_id"].dtype
    == dtype("int64"),
}

if __name__ == "__main__":
    pass
