import pandas as pd
import datetime
import numpy as np

def _get_notification_ratio(df, config, place_col):
    """
    Calculate city/state notification rate = CFR * I(t) / D(t)
    
    1. Get daily 1/death_ratio = I(t) / D(t)
    2. Calculate mavg of 7 days = CFR / death_ratio
    """
    
    cfr = config["br"]["seir_parameters"]["fatality_ratio"]
    
    rate = df.groupby([place_col, "last_updated"]).sum()\
            .reset_index()\
            .set_index("last_updated", drop=True)\
            .groupby(place_col)\
            .apply(lambda x: x["confirmed_cases"] / x["deaths"])\
            .rolling(window=7, min_periods=1)\
            .apply(lambda x: np.mean(x) * cfr, raw=True)\
            .reset_index() 

    return rate

def _adjust_subnotification_cases(df, config):

    # Calcula taxa de notificação por cidade / estado
    df_city = _get_notification_ratio(df, config, "city_id")\
        .rename({0: "city_notification_rate"}, axis=1)
    df_state = _get_notification_ratio(df, config, "state")\
        .rename({0: "state_notification_rate"}, axis=1)
    
    df = df.merge(df_city, on=["city_id", "last_updated"])\
           .merge(df_state, on=["state", "last_updated"])
    
    # Escolha taxa de notificação para a cidade: caso sem mortes, usa taxa UF
    df["notification_rate"] = df["city_notification_rate"]
    df["notification_rate"] = np.where(df["notification_rate"].isnull(), 
                                       df["state_notification_rate"],
                                       df["city_notification_rate"])
    # Ajusta caso taxa > 1:
    df["notification_rate"] = np.where(df["notification_rate"] > 1, 
                                        1, 
                                        df["notification_rate"])
    
    return df[["city_id", "state_notification_rate", "notification_rate", "last_updated"]].drop_duplicates()


def _get_active_cases(df, window_period, cases_params):
    
    # Soma casos diários dos últimos dias de progressão da doença
    daily_active_cases = df.set_index("last_updated")\
                        .groupby("city_id")["daily_cases"]\
                        .rolling(min_periods=1, window=window_period)\
                        .sum().reset_index()

    df = df.merge(
        daily_active_cases, 
        on=["city_id", "last_updated"], 
        suffixes=("", "_sum")
    ).rename(columns=cases_params["rename"])

    return df

def _correct_cumulative_cases(df):

    # Corrije acumulado para o valor máximo até a data
    df['confirmed_cases'] = df.groupby('city_id')\
                              .cummax()['confirmed_cases']

    # Recalcula casos diários
    df['daily_cases'] = df.groupby('city_id')['confirmed_cases']\
                          .diff(1)

    # Ajusta 1a dia para o acumulado
    df['daily_cases'] = np.where(
                            df['daily_cases'].isnull() == True, 
                            df['confirmed_cases'], 
                            df['daily_cases']
                        )
    return df

def now(country, config, last=True):

    if country == "br":
        df = pd.read_csv(config[country]["cases"]["url"])
        df = df.query("place_type == 'city'").dropna(subset=["city_ibge_code"]).fillna(0)

        cases_params = config["br"]["cases"]
        df = df.rename(columns=cases_params["rename"])
        df["last_updated"] = pd.to_datetime(df["last_updated"])
        
        # Corrije dados acumulados
        df = _correct_cumulative_cases(df)

        # Calcula casos ativos estimados
        
        # 1. Calcula casos ativos = novos casos no período de progressão
        infectious_period = config["br"]["seir_parameters"]["severe_duration"] + \
                    config["br"]["seir_parameters"]["critical_duration"]

        df = _get_active_cases(df, infectious_period, cases_params).rename(
            columns=cases_params["rename"]
        )

        df = df.merge(
            _adjust_subnotification_cases(df, config), 
            on=["city_id", "last_updated"]
        )
        # 2. Ajusta pela taxa de subnotificacao: quando não tem morte ainda na UF, não ajustamos
        df["active_cases"] = np.where(
                                df["notification_rate"].isnull(),
                                round(df["infectious_period_cases"], 0),
                                round(df["infectious_period_cases"] / df["notification_rate"], 0)
                            )

        if last:
            df = df[df["is_last"] == last].drop(cases_params["drop"], 1)
        
        df["city_id"] = df["city_id"].astype(int)

    return df


if __name__ == "__main__":

    pass
