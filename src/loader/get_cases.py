import pandas as pd
import datetime
import numpy as np

def _get_active_cases(df, window_period, cases_params):
    
    # Soma casos diários dos últimos dias de progressão da doença
    daily_active_cases = df.set_index('last_updated')\
                                .groupby('city_id')['daily_cases']\
                                .rolling(min_periods=1, window=window_period)\
                                .sum().reset_index()

    df = df.merge(daily_active_cases, 
                on=['city_id', 'last_updated'], 
                suffixes=('', '_sum')).rename(columns=cases_params['rename'])

    return df

def _get_notification_rate(group, config):
    
    daily_adjust = group['deaths'] / config['br']['seir_parameters']['fatality_ratio']
    notification_rate = np.mean(group['confirmed_cases'] / daily_adjust)
    
    return notification_rate

def _adjust_subnotification_cases(df, cases_params, config):
    
    # Filtra dataframe dos dias para o cálculo
    notification_day = df['last_updated'].max() - datetime.timedelta(cases_params['notification_window_days'])
    df = df[df['last_updated'] > notification_day]

    # Calcula taxa de notificação por cidade
    city_notif_rate = df.groupby('city_id').apply(lambda x: _get_notification_rate(x, config))\
                        .reset_index().rename({0: 'city_notification_rate'}, axis=1)

    # Calcula taxa de notificação por estado
    state_notif_rate = df.groupby(['state', 'last_updated']).sum()\
                         .reset_index().groupby('state')\
                         .apply(lambda x: _get_notification_rate(x, config)).reset_index()\
                         .rename({0: 'state_notification_rate'}, axis=1)
    
    df = df.merge(city_notif_rate, on='city_id').merge(state_notif_rate, on='state')
    
    # Escolha taxa de notificação para a cidade: caso sem mortes, usa taxa UF (UF sem mortes => 1)
    df['notification_rate'] = np.where(abs(df['city_notification_rate']) != np.inf, 
                                       df['city_notification_rate'],
                                       np.where(abs(df['state_notification_rate']) != np.inf, 1, 
                                                df['state_notification_rate']))

    return df[['city_id', 'notification_rate']].drop_duplicates()

def now(country, config):

    if country == 'br':
        df = pd.read_csv(config[country]['cases']['url'])
        df = df.query('place_type == "city"').dropna(subset=['city_ibge_code'])

        cases_params = config['br']['cases']
        df = df.rename(columns=cases_params['rename'])
        df['last_updated'] = pd.to_datetime(df['last_updated'])

        infectious_period = config['br']['seir_parameters']['severe_duration'] + \
                            config['br']['seir_parameters']['critical_duration']

        # Calcula casos ativos
        df = _get_active_cases(df, infectious_period, cases_params).rename(columns=cases_params['rename'])

        # Ajusta subnotificação de casos
        df = df.merge(_adjust_subnotification_cases(df, cases_params, config), on='city_id')

        df['active_cases'] = df['infectious_period_cases'] / df['notification_rate']

        # # Calcula recuperados
        # df['recovered'] = df['confirmed_cases'] - df['active_cases'] - df['deaths']
        # df['recovered'] = np.where(df['recovered'] < 0, df['confirmed_cases'] - df['active_cases'], df['recovered'])

        df = df[df['is_last'] == True].drop(cases_params['drop'], 1)
        df['city_id'] = df['city_id'].astype(int)

    return df

if __name__ == "__main__":

    pass