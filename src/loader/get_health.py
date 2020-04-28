import pandas as pd

from utils import download_from_drive, treat_text


def _read_cities_data(country, config):

    paths = config[country]['drive_paths']

    return {name: download_from_drive(url) for name, url in paths.items()}


def now(country, config):
    
    cities = _read_cities_data(country, config)
    cities = pd.merge(
        cities['cities_population'], cities['health_infrastructure'],
        on='city_id', how='left', suffixes=('', '_y'))
    cities = cities.drop([c for c in cities.columns if '_y' in c], 1)

    cities[['city_norm']] = cities[['city_name']].apply(treat_text)

    time_cols = [c for c in cities.columns if 'last_updated' in c]
    cities[time_cols] = cities[time_cols].apply(pd.to_datetime)
    
    return cities


if __name__ == "__main__":

    pass