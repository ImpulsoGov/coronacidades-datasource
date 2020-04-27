import pandas as pd
import requests
from copy import deepcopy
import os
import yaml
import pytest
import json
from datetime import datetime

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

def _read_data(config):

    cases = get_cases.now('br', config)
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

    output_path = '/'.join([os.getenv('OUTPUT_DIR'),
                           os.getenv('RAW_NAME')]) + '.csv'

    config_url = os.getenv('CONFIG_URL')
    config = yaml.load(
        requests.get(config_url).text,
        Loader=yaml.FullLoader)

    data = _read_data(config)

    data['data_last_refreshed'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    data.to_csv(output_path, index=False)

if __name__ == "__main__":

    main()