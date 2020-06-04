# code originally from (only minor updates):
# https://github.com/loft-br/realtime_r0_brazil/blob/master/realtime_r0_bettencourt_ribeiro.ipynb

import numpy as np
import pandas as pd
import datetime as dt
from scipy import stats as sps
from joblib import Parallel, delayed
from utils import get_cases_series
from endpoints import get_cases
from loguru import logger

from endpoints.helpers import allow_local


def smooth_new_cases(new_cases, params):

    """
    Function to apply gaussian smoothing to cases

    Arguments
    ----------
    new_cases: time series of new cases

    Returns 
    ----------
    smoothed_cases: cases after gaussian smoothing

    See also
    ----------
    This code is heavily based on Realtime R0
    by Kevin Systrom
    https://github.com/k-sys/covid-19/blob/master/Realtime%20R0.ipynb
    """

    smoothed_cases = (
        new_cases.rolling(
            params["window_size"], win_type="gaussian", min_periods=1, center=True
        )
        .mean(std=params["gaussian_kernel_std"])
        .round()
    )

    zeros = smoothed_cases.index[smoothed_cases.eq(0)]

    if len(zeros) == 0:
        idx_start = 0
    else:
        last_zero = zeros.max()
        idx_start = smoothed_cases.index.get_loc(last_zero) + 1

    smoothed_cases = smoothed_cases.iloc[idx_start:]

    return smoothed_cases


def calculate_posteriors(sr, params, serial_interval):

    """
    Function to calculate posteriors of Rt over time

    Arguments
    ----------
    sr: smoothed time series of new cases

    sigma: gaussian noise applied to prior so we can "forget" past observations
           works like exponential weighting

    Returns 
    ----------
    posteriors: posterior distributions
    log_likelihood: log likelihood given data

    See also
    ----------
    This code is heavily based on Realtime R0
    by Kevin Systrom
    https://github.com/k-sys/covid-19/blob/master/Realtime%20R0.ipynb
    """

    params["r_t_range"] = np.linspace(
        0, int(params["r_t_range_max"]), int(params["r_t_range_max"]) * 100 + 1
    )

    # (1) Calculate Lambda
    lam = sr[:-1].values * np.exp((params["r_t_range"][:, None] - 1) / serial_interval)

    # (2) Calculate each day's likelihood
    likelihoods = pd.DataFrame(
        data=sps.poisson.pmf(sr[1:].values, lam),
        index=params["r_t_range"],
        columns=sr.index[1:],
    )

    # (3) Create the Gaussian Matrix
    process_matrix = sps.norm(
        loc=params["r_t_range"], scale=params["optimal_sigma"]
    ).pdf(params["r_t_range"][:, None])

    # (3a) Normalize all rows to sum to 1
    process_matrix /= process_matrix.sum(axis=0)

    # (4) Get prior
    prior0 = sps.gamma(a=params["gamma_alpha"]).pdf(params["r_t_range"])
    prior0 /= prior0.sum()

    # Create a DataFrame that will hold our posteriors for each day
    posteriors = pd.DataFrame(
        index=params["r_t_range"], columns=sr.index, data={sr.index[0]: prior0}
    )

    # (5) Iteratively apply Bayes' rule
    for previous_day, current_day in zip(sr.index[:-1], sr.index[1:]):

        # (5a) Calculate the new prior
        current_prior = process_matrix @ posteriors[previous_day]

        # (5b) Calculate the numerator of Bayes' Rule: P(k|R_t)P(R_t)
        numerator = likelihoods[current_day] * current_prior

        # (5c) Calcluate the denominator of Bayes' Rule P(k)
        denominator = np.sum(numerator)

        # Execute full Bayes' Rule
        posteriors[current_day] = numerator / denominator

    return posteriors


def highest_density_interval(pmf, p=0.95):
    """
    Function to calculate highest density interval 
    from posteriors of Rt over time

    Arguments
    ----------
    pmf: posterior distribution of Rt

    p: mass of high density interval

    Returns 
    ----------
    interval: expected value and density interval

    See also
    ----------
    This code is heavily based on Realtime R0
    by Kevin Systrom
    https://github.com/k-sys/covid-19/blob/master/Realtime%20R0.ipynb
    """

    # If we pass a DataFrame, just call this recursively on the columns
    if isinstance(pmf, pd.DataFrame):
        return pd.DataFrame(
            [highest_density_interval(pmf[col], p=p) for col in pmf], index=pmf.columns
        )

    cumsum = np.cumsum(pmf.values)

    # N x N matrix of total probability mass for each low, high
    total_p = cumsum - cumsum[:, None]

    # Return all indices with total_p > p
    lows, highs = (total_p > p).nonzero()

    # Find the smallest range (highest density)
    best = (highs - lows).argmin()

    low = pmf.index[lows[best]]
    high = pmf.index[highs[best]]
    most_likely = pmf.idxmax(axis=0)

    interval = pd.Series(
        [most_likely, low, high],
        index=["Rt_most_likely", f"Rt_low_{p*100:.0f}", f"Rt_high_{p*100:.0f}"],
    )
    return interval


def run_full_model(cases, config):

    # smoothing series
    smoothed = smooth_new_cases(cases, config["br"]["rt_parameters"],)

    # calculating posteriors
    posteriors = calculate_posteriors(
        smoothed,
        config["br"]["rt_parameters"],
        config["br"]["seir_parameters"]["mild_duration"] * 0.5
        + config["br"]["seir_parameters"]["incubation_period"],
    )

    # calculating HDI
    result = highest_density_interval(posteriors, p=0.95)

    return result


def parallel_run(df, config, place_type="city_id"):

    results = [run_full_model(gr[1], config) for gr in df.groupby(level=place_type)]
    
    return pd.concat(results).reset_index()


@allow_local
def now(config):

    # Import cases
    df = get_cases.now(config, "br")
    df["last_updated"] = pd.to_datetime(df["last_updated"])

    # Filter more than 14 days
    df = get_cases_series(df, "city_id", config["br"]["rt_parameters"]["min_days"])

    # Run in parallel
    return parallel_run(df, config, place_type="city_id")


TESTS = {
    "data is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
}
