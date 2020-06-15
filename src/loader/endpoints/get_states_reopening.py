import pandas as pd
from utils import treat_text, download_from_drive
from endpoints.helpers import allow_local
from numpy import dtype


@allow_local
def now(config, country="br"):
    updates = download_from_drive(config[country]["drive_paths"]["reopening_data"])
    # updates = download_from_drive("https://docs.google.com/spreadsheets/d/1Jc1M-F5cM_tLId5jnn5ojzgXtKgqqVVDmXVqGBwZQ5U")
    return updates


TESTS = {
    "data is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    "null datafield exists": lambda df: df.isnull().sum().sum() == 0,
    "CNAE field is not exclusively ints": lambda df: df["cnae"].dtype == dtype("int64"),
    "UF field is not exclusively ints": lambda df: df["uf"].dtype == dtype("int64"),
}

if __name__ == "__main__":

    pass
