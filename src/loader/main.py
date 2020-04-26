import pandas as pd
import requests
from copy import deepcopy
import os
import yaml
import pytest

import get_cases, get_embaixadores, get_health
from utils import get_last

def _get_supplies(cities, updates, country, config):
    
    final_cols = config[country]['columns']['final']
    
    final = []
    for h in config[country]['columns']['health']:
        u = updates.rename(columns={'name': 'author'
                                   })[final_cols + [h]].dropna(subset=[h])
        u = get_last(u)
        cities['author'] = config[country]['health']['source']
        c = cities.rename(columns={'last_updated_' + h: 'last_updated'})[final_cols + [h]]
        c[h] = c[h] * config[country]['health']['initial_proportion']
        f = get_last(pd.concat([c, u]))
        f.columns = ['city_id'] + [i + '_' + h for i in final_cols if i != 'city_id'] + [h]
        final.append(deepcopy(f))
        
    supplies = pd.concat(final, 1)
    supplies = supplies.loc[:,~supplies.columns.duplicated()]
    
    return supplies

def _read_data(country, config):

    cases = get_cases.now(country, config)
    
    if country == 'br':

        updates = get_embaixadores.now('br', config)
        cities = get_health.now('br', config)

        updates = cities[['state_id', 'city_norm', 'city_id']]\
                .merge(updates, on=['state_id', 'city_norm'], how='right')

        supplies = _get_supplies(cities, updates, 'br', config)

        # merge cities
        df = cities[
                ['country_iso',
                'country_name',
                'state_id',
                'state_name',
                'city_id',
                'city_name',
                'population',
                'health_system_region',
                ]].merge(supplies, on='city_id')

        # merge cities
        df = df.merge(cases, on='city_id', how='left')


    return df

def main():

    output_dir = os.getenv('OUTPUT_DIR')

    config = yaml.load(
        requests.get('https://raw.githubusercontent.com/ImpulsoGov/simulacovid/master/src/configs/config.yaml').text)

    read_data('br', config).to_csv(output_dir)

if __name__ == "__main__":

    exc = pytest.main(['tests', '--resultlog=test.txt'])

    print('oi')