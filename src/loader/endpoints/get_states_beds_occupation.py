from urllib.error import HTTPError

import pandas as pd

from endpoints.helpers import allow_local
from logger import logger


UF = [
    "Acre",
    "Alagoas",
    "Amapá",
    "Amazonas",
    "Bahia",
    "Ceará",
    "Distrito Federal",
    "Espírito Santo",
    "Goiás",
    "Maranhão",
    "Mato Grosso",
    "Mato Grosso do Sul",
    "Minas Gerais",
    "Pará",
    "Paraíba",
    "Paraná",
    "Pernambuco",
    "Piauí",
    "Rio de Janeiro",
    "Rio Grande do Norte",
    "Rio Grande do Sul",
    "Rondônia",
    "Roraima",
    "Santa Catarina",
    "São Paulo",
    "Sergipe",
    "Tocantins",
]


def download_giscard_tables():
    """Fetches data from Giscard's 'Painel COVID-19'."""
    giscard = pd.DataFrame()

    for ix, uf in enumerate(UF):
        for attempt in range(1, 3):
            try:
                giscard_uf = pd.read_csv(
                    "http://www.giscard.com.br/coronavirus/arquivos"
                    + "/painel-covid19-giscard-{}.csv".format(ix + 1001),
                    parse_dates=["Data Casos"],
                    dayfirst=True,
                    usecols=["Data Casos", "Ocupacao Leitos UTI"],
                )
            except HTTPError:
                continue
            else:
                giscard_uf["state_name"] = uf
                giscard_uf["author_icu_occupation"] = "Giscard"
                giscard = pd.concat([giscard, giscard_uf], ignore_index=True)
                break

    return (
        giscard
        .replace(0, pd.NA)
        .rename(columns={
            "Data Casos": "last_updated",
            "Ocupacao Leitos UTI": "icu_occupation_rate",
        })
    )


@allow_local
def now(config):
    logger.info("Baixando dados de ocupação hospitalar")
    places_ids = pd.read_csv(
            "http://datasource.coronacidades.org/br/places/ids",
            usecols=["state_id", "state_name", "state_num_id"],
    ).drop_duplicates()
    df_occupation = download_giscard_tables()
    return places_ids.merge(df_occupation, on="state_name")


TESTS = {
    "df is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    "doesnt have 27 states": lambda df: len(df["state_num_id"].unique()) == 27,
    "duplicated state and date": (
        lambda df: df.duplicated(["state_num_id", "last_updated"]).sum() == 0
    ),
    "missing required fields": (
        lambda df: df[[
            "state_name",
            "state_id",
            "state_num_id",
            "last_updated",
        ]].notnull().values.all()
    )
}
