import pandas as pd
import datetime
import numpy as np
from urllib.request import Request, urlopen
import json
import time

import gzip
import io
from urllib.request import Request, urlopen

from endpoints.helpers import allow_local
from utils import download_from_drive


@allow_local
def now(config, country="br"):

    return download_from_drive(
        config[country]["drive_paths"]["br_id_state_region_city"]
    )


TESTS = {
    "not 5570 cities": lambda df: len(df["city_id"].unique()) == 5570,
    "not 27 states": lambda df: len(df["state_id"].unique()) == 27,
    "not all states have unique id": lambda df: len(
        df[["state_num_id", "state_id", "state_name"]].drop_duplicates()
    )
    == 27,
    "not all cities have unique id": lambda df: len(
        df[["city_id", "city_name"]].drop_duplicates()
    )
    == 5570,
    "df is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
}


# ===> Generation code
# import pandas as pd
# if __name__ == "__main__":
#     # get ibge 7 digits code match & state name: https://www.ibge.gov.br/explica/codigos-dos-municipios.php
#     cod_ibge_6to7d = (pd.read_excel("RELATORIO_DTB_BRASIL_MUNICIPIO.xls", sep=";")
#                           .rename(columns={"Código Município Completo": "city_id",
#                                            "Nome_UF": "state_name"})
#                           .assign(city_id6d=lambda df: df["city_id"].apply(lambda x: int(str(x)[:-1])))
#                           .set_index("city_id6d")[["city_id", "state_name"]])
#     # get & treat default ids: https://sage.saude.gov.br/?link=paineis/regiaoSaude/corpao&flt=false&param=null&ufibge=&municipioibge=&cg=&tc=&re_giao=&rm=&qs=&idPagina=83#
#     df = (pd.read_csv("lista_dados_sage.csv", sep=";")
#             .rename(columns={"uf": "state_id",
#                  "cidade": "city_name",
#                  "ibge": "city_id6d",
#                  "regiao": "health_region_id",
#                  "no_regiao": "health_region_name"
#                 })
#             .set_index("city_id6d", drop=True)
#             .assign(city_id=cod_ibge_6to7d["city_id"])
#             .assign(state_name=cod_ibge_6to7d["state_name"])
#             .assign(state_num_id=lambda df: df["city_id"].apply(lambda x: int(str(x)[:2])))
#             .reset_index(drop=True)
#         )
#     df.to_csv("br_sage_default_place_ids.csv")
#     print("Done!")
