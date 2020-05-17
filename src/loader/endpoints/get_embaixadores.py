import pandas as pd
from utils import treat_text, download_from_drive


def now(config, country="br"):

    # print(config[country]["drive_paths"]["embaixadores"])
    updates = download_from_drive(config[country]["drive_paths"]["embaixadores"])

    # change column names
    updates.columns = [
        "timestamp",
        "email",
        "city_norm",
        "state_id",
        "name",
        "last_updated",
        "number_ventilators",
        "number_beds",
        "n_casos",
        "n_mortes",
        "number_icu_beds",
        "number_available_ventilators",
        "number_tota_icu_beds",
        "source",
    ]

    # treat text
    c = ["city_norm"]
    updates[c] = updates[c].apply(treat_text)

    # treat timestamp
    updates["last_updated"] = updates["timestamp"].apply(pd.to_datetime)

    return updates


TESTS = {
    "data is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
}

if __name__ == "__main__":

    pass
