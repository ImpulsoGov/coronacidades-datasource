import subprocess
import os
import pandas as pd
import time
import os
import subprocess
from endpoints.helpers import allow_local
from endpoints import get_cases
import json
import pickle


@allow_local
def now(config=None):

    # Write temp file
    rscript = "Rscript /app/src/endpoints/scripts/test.R"

    # TODO: fix config type
    p = subprocess.Popen(
        rscript,
        shell=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        # encoding="utf-16",
    )

    df, err = p.communicate(input=(pickle.dumps(get_cases.now(config))))

    df = [x.split(",") for x in df.decode("utf-8").split('"')[1].split("\\n")]
    rc = p.returncode

    return pd.DataFrame(df[1:][:-1], columns=df[0])


TESTS = {
    "data is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    "dataframe has null data": lambda df: all(df.isnull().any() == False),
    "not all 27 states with updated rt": lambda df: len(
        df.drop_duplicates("state_num_id", keep="last")
    )
    == 27,
    "rt most likely outside confidence interval": lambda df: len(
        df[
            (df["Rt_most_likely"] >= df["Rt_high_95"])
            & (df["Rt_most_likely"] <= df["Rt_high_95"])
        ]
    )
    == 0,
    "state has rt with less than 14 days": lambda df: all(
        df.groupby("state_num_id")["last_updated"].count() > 14
    )
    == True,
}
