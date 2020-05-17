from utils import get_cases_series
from endpoints import get_cases, get_cities_rt
import pandas as pd
import numpy as np


def now(config, last=False):

    config["br"]["rt_parameters"] = {
        "r_t_range_max": 12,
        "optimal_sigma": 0.01,  # best sigma for Brazil (prior hyperparameters)
        "window_size": 7,
        "gaussian_kernel_std": 2,
        "gaussian_min_periods": 7,
        "gamma_alpha": 4,
        "min_days": 14,
    }

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
