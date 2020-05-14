# general imports
import numpy as np
import pandas as pd
import datetime as dt

# urls
CITY_DATA_URL = 'https://raw.githubusercontent.com/wcota/covid19br/master/cases-brazil-cities-time.csv'
STATE_DATA_URL = 'https://raw.githubusercontent.com/wcota/covid19br/master/cases-brazil-cities-time.csv'


def load_data():

    """
    Loads state and city data from wcota repository

    Returns
    ----------
    city_df: city data
    state_df: state data (Brazil)

    """

    city_df = (
                pd.read_csv(CITY_DATA_URL, parse_dates=['date'])
                .rename(columns={'totalCases':'confirmed_total',
                                 'newCases': 'confirmed_new',
                                 'deaths': 'deaths_total',
                                 'newDeaths': 'deaths_new'})
                .drop(['ibgeID','country','state'], axis=1)
                .assign(city = lambda x: x['city'].replace('TOTAL', 'Brazil'))
                .groupby(['city','date']).sum()
            )

    state_df = (
                pd.read_csv(STATE_DATA_URL, parse_dates=['date'])
                .rename(columns={'totalCases':'confirmed_total',
                                 'newCases': 'confirmed_new',
                                 'deaths': 'deaths_total',
                                 'newDeaths': 'deaths_new'})
                .drop(['ibgeID','country','city',
                       'deaths_per_100k_inhabitants',
                       'totalCases_per_100k_inhabitants',
                       'deaths_by_totalCases'], axis=1)
                .assign(state = lambda x: x['state'].replace('TOTAL', 'Brazil'))
                .groupby(['state','date']).sum()
                )

    return city_df, state_df

def tidy_raw_time_series_data(df_raw, index_str):
  
  # setting index for time series
  df = (
         df_raw
         .set_index(['Province/State','Country/Region','Lat','Long'])
        )

  # creating multi index for slicing
  # also converting dates to datetime
  df.columns = pd.MultiIndex.from_product([[index_str], pd.to_datetime(df.columns)])
  df = df.sort_index(axis=1).stack()
  
  return df

def load_johns_hopkins_data():
  
  # downloading data for confirmed, deaths and recoveries
  confirmed_raw=pd.read_csv('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv')
  deaths_raw=pd.read_csv('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv')
  
  # tidying the data
  confirmed = tidy_raw_time_series_data(confirmed_raw, 'confirmed')
  deaths = tidy_raw_time_series_data(deaths_raw, 'deaths')
  
  # let us concat these dfs and then we're ready
  df = (
        pd.concat([confirmed, deaths], axis=1)
        .reset_index()
        .rename(columns={'level_4':'date', 
                         'Province/State':'province',
                         'Country/Region':'country',
                         'confirmed':'confirmed_total',
                         'deaths':'deaths_total'})
        .drop(['Lat', 'Long', 'province'], axis=1)
        .query('confirmed_total != 0')
        .groupby(['country','date']).sum()
       )
  
  # calculating new cases
  df['confirmed_new'] = df['confirmed_total'].groupby('country').diff()
  df['deaths_new'] = df['deaths_total'].groupby('country').diff()
  
  return df

def load_data_us():
    url = 'https://covidtracking.com/api/v1/states/daily.csv'
    state_df = pd.read_csv(url,
                        usecols=['date', 'state', 'positive'],
                        parse_dates=['date'],
                        index_col=['state', 'date'],
                        squeeze=True).sort_index()

    state_df = (state_df
                .to_frame()
                .rename(columns={'positive':'confirmed_total'})
                .loc[lambda x: x['confirmed_total'] > 0])

    state_df['confirmed_new'] = state_df['confirmed_total'].groupby(level='state').diff()
    state_df = state_df.dropna().clip(lower=0)

    return state_df