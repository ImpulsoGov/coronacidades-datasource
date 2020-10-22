import pandas as pd

from endpoints import get_cities_farolcovid_main
from utils import download_from_drive

from endpoints.helpers import allow_local


@allow_local
def now(config):
    """
    Tabela auxiliar de número de alunos por ano escolar e combinações possíveis de filtros para calculadora.

    Parameters
    ----------
    config : dict
    """

    return download_from_drive(
        "https://docs.google.com/spreadsheets/d/1aa0WJ2lF3mKn_Tf6n-Te7NWp2KQN8gFLJiYXwRz6xNM"
    ).merge(
        get_cities_farolcovid_main.now()[["state_id", "city_name", "city_id"]],
        on=["city_id"],
    )


# Output dataframe tests to check data integrity. This is also going to be called
# by main.py
TESTS = {
    "more than 5570 cities": lambda df: len(df["city_id"].unique()) <= 5570,
    "data is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    "dataframe has null data": lambda df: all(df.isnull().any() == False),
}


# ==> Code from original data: See get_cities_safeschools_main
