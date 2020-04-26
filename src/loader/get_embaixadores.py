import pandas as pd
from utils import treat_text, download_from_drive


def now(country, config):
    
    updates = download_from_drive(config[country]['drive_paths']['embaixadores'])
    
    # change column names
    updates.columns = [
        'timestamp',
        'email',
        'city_norm',
        'state_id',
        'name',
        'last_updated',
        'number_ventilators',
        'number_beds',
        'n_casos',
        'n_mortes',
        'number_icu_beds'
    ]
    
    # treat text
    c = ['city_norm']
    updates[c] = updates[c].apply(treat_text)

    # treat timestamp
    updates['last_updated'] = updates['timestamp'].apply(pd.to_datetime)
    
    return updates


if __name__ == "__main__":

    pass