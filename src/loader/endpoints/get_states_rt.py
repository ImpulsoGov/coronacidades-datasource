from utils import get_cases_series
from endpoints import get_cases, get_cities_rt
import pandas as pd
import numpy as np

from endpoints.helpers import allow_local


@allow_local
def now(config, last=False):

    # Import cases
    df = get_cases.now(config, "br", last)
    df["last_updated"] = pd.to_datetime(df["last_updated"])

    # Filter more than 14 days
    df = get_cases_series(df, "state", config["br"]["rt_parameters"]["min_days"])

    # Run in parallel
    return get_cities_rt.parallel_run(df, config, place_type="state")


TESTS = {
    "data is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
}
