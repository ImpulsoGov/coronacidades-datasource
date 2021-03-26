import os
import requests
import yaml
import gzip
import io
from urllib.request import Request, urlopen
import pandas as pd
from endpoints.helpers import allow_local
from utils import download_from_drive

def download_brasilio_table(dataset="covid19", table_name="microdados_vacinacao"):
    """
    Baixa dados completos do Brasil.io e retorna CSV.
    """
    head = {"User-Agent": "python-urllib/brasilio-client-0.1.0"}
    request = Request("https://data.brasil.io/dataset/covid19/microdados_vacinacao.csv.gz", headers=head)
    response = urlopen(request)
    return io.TextIOWrapper(gzip.GzipFile(fileobj=response), encoding="utf-8")

@allow_local
def now(config):
    cols = {
        "estabelecimento_codigo_ibge_municipio": "int",
        "paciente_uuid": "str",
        "numero_dose": "str"
    }
    df = pd.read_csv(download_brasilio_table(),usecols=cols.keys()).groupby(["estabelecimento_codigo_ibge_municipio", "numero_dose"]).agg({"paciente_uuid": 'count'})
    df = df.reset_index()
    df['estabelecimento_codigo_ibge_municipio'] = df['estabelecimento_codigo_ibge_municipio'].astype(int)
    df['numero_dose'] = df['numero_dose'].astype(int)
    df_pop_city = download_from_drive(config["br"]["drive_paths"]["cities_population"])[
        [
            "country_iso",
            "country_name",
            "state_id",
            "state_name",
            "city_id",
            "city_name",
            "population",
        ]
    ]
    places_id = pd.read_csv("http://datasource.coronacidades.org/br/places/ids")
    df_places = df.merge(places_id, right_on="city_id", left_on="estabelecimento_codigo_ibge_municipio")
    df_group_city = df_places.merge(df_pop_city[{"city_id", "population"}], on="city_id")
    df_group_city['imunizados'] = (df_group_city[df_group_city["numero_dose"] == 2]['paciente_uuid']).astype(int)
    df_group_city['vacinados'] = (df_group_city[df_group_city["numero_dose"] == 1]['paciente_uuid']).astype(int)

    # REGIAO
    df_group_region = df_group_city.groupby(['health_region_id', 'health_region_name', 'state_id', 'state_name','state_num_id']).agg({"population":"sum","vacinados":"sum","imunizados":"sum"})
    df_group_region = df_group_region.reset_index()
    df_group_region['perc_imunizados'] = round(df_group_region['imunizados']/df_group_region['population']*100, 2).fillna(0)
    df_group_region['perc_vacinados'] = round(df_group_region['vacinados']/df_group_region['population']*100, 2).fillna(0)
    df_group_region['nao_vacinados'] = (df_group_region['population']-df_group_region['vacinados']).astype(int)
    df_group_region['last_updated'] = pd.to_datetime('now').strftime("%d/%m/%Y")
    
    return df_group_region


TESTS = {
    "more than 450 regions": lambda df: len(df["health_region_id"].unique()) <= 450,
    "quantidade da populacao zerada ou negativa": lambda df: len(df["population"].unique()) > 0,
    "numero de vacinados zerados ou negativo": lambda df: len(df["vacinados"].unique()) > 0
}