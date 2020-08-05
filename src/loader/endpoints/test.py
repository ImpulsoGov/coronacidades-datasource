import subprocess
import os
import pandas as pd
import time
import os
import subprocess
from endpoints.helpers import allow_local
import json


@allow_local
def now(config):

    # Write temp file
    rscript = "Rscript /app/src/endpoints/aux/test.R"

    params = json.dumps({"mean_si": 4.7, "std_si": 2.9, "mean_prior": 3,}).encode(
        "utf-16"
    )

    p = subprocess.Popen(
        rscript,
        shell=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        # encoding="utf-16",
    )

    df, err = p.communicate(input=(params))
    print(err, df)
    df = pd.read_csv(df)
    rc = p.returncode

    time.sleep(1)

    return pd.read_csv(df)


TESTS = {
    "data is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    "dataframe has null data": lambda df: all(df.isnull().any() == False),
    "not all 27 states with updated rt": lambda df: len(
        df.drop_duplicates("state", keep="last")
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
        df.groupby("state")["last_updated"].count() > 14
    )
    == True,
}
