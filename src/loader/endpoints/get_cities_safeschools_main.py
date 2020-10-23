import pandas as pd

from endpoints import get_cities_farolcovid_main
from utils import download_from_drive

from endpoints.helpers import allow_local


@allow_local
def now(config):
    """
    Gera tabela de número de salas, professores e alunos para cada combinação possível de filtro.

    Parameters
    ----------
    config : dict
    """

    return download_from_drive(
        "https://docs.google.com/spreadsheets/d/139oQZg-anzAnLsVoHJHLci6pnw-6rNPhZVSYZ-Ynl_A"
    ).merge(
        get_cities_farolcovid_main.now(config)[
            ["state_id", "city_name", "city_id", "overall_alert", "last_updated_cases"]
        ],
        on=["city_id"],
    )


# Output dataframe tests to check data integrity. This is also going to be called
# by main.py
TESTS = {
    "more than 5570 cities": lambda df: len(df["city_id"].unique()) <= 5570,
    "data is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    "dataframe has null data": lambda df: all(
        df.drop(columns=["overall_alert", "last_updated_cases"]).isnull().any() == False
    ),
}

# ==> Code from original School Census data:

# import pandas as pd
# import numpy as np

# # Treatment functions & parameters
# replaces = {
#     "IN_ESPECIAL_EXCLUSIVA": {
#         0: "Não excluiva"
#     },
#     "TP_SITUACAO_FUNCIONAMENTO": {
#         1: "Ativa"
#     },
#     "TP_TIPO_ATENDIMENTO_TURMA": {
#         1: "Escolarização",
#         2: "Escolarização",
#     },
#     "TP_TIPO_DOCENTE": {
#         1: "Docente"
#     },
#     "TP_ETAPA_ENSINO": {
#         14: "FAI1",
#         15: "FAI2",
#         16: "FAI3",
#         17: "FAI4",
#         18: "FAI5",
#         56: "FAI6",
#         19: "FAF6",
#         20: "FAF7",
#         21: "FAF8",
#         41: "FAF9",
#         25: "EM1",
#         26: "EM2",
#         27: "EM3",
#         28: "EM4",
#     },
#     "TP_DEPENDENCIA": {
#         2: "Estadual",
#         3: "Municipal",
#     },
#     "TP_LOCALIZACAO": {
#         1: "Urbana",
#         2: "Rural",
#     },
#     "IN_AGUA_REDE_PUBLICA": {
#         0: "Não",
#         1: "Sim",
#     }
# }

# levels = {
#     "FAI": "Fundamental - Anos iniciais",
#     "FAF": "Fundamental - Anos finais",
#     "EM": "Ensino Médio"
# }

# def _gen_level_column(df, levels):
#     return (
#     df.rename(columns={"TP_ETAPA_ENSINO": "TP_ETAPA_ANO"})
#     .assign(TP_ETAPA_ENSINO= lambda df: df["TP_ETAPA_ANO"]
#             .apply(lambda x: levels[x[:-1]])))


# def treat_columns(df, replaces, levels):
#     for col in replaces:
#         if col in df.columns:
#             df = df.query(f"{col} in {list(replaces[col].keys())}")
#             df[col] = df[col].replace(replaces[col])

#     if "TP_ETAPA_ENSINO" in df.columns:
#         df = _gen_level_column(df, levels)

#     return df

# # Aggregate functions
# from itertools import chain, combinations

# def all_filters_choices(filters):
#     "powerset([1,2,3]) --> () (1,) (2,) (3,) (1,2) (1,3) (2,3) (1,2,3)"
#     s = list(filters)
#     return list(chain.from_iterable(combinations(s, r) for r in range(len(s)+1)))


# def agg_by_choice(df, ids, filters, agg_col, melt=None):
#     """
#     Agrega dados de uma coluna por id e todas as combinações possíveis de filtros.
#     """

#     all_filters = all_filters_choices([col for col in filters if col in df.columns])

#     result = pd.DataFrame()
#     for choice in all_filters:

#         if agg_col == "QT_SALAS_UTILIZADAS":

#             print("aqui!")

#             melt_col = "TP_ETAPA_ENSINO" # nome da coluna fundida
#             id_vars = ids + list(choice) + [agg_col] # colunas para manter

#             df_melt = (
#                 df[
#                     id_vars + list(melt.keys())
#                 ]
#                 # funde indicadores de etapa
#                 .melt(id_vars=id_vars,
#                       var_name=[melt_col],
#                       value_name='value')
#                 .query("value == 1")
#                 .drop(columns="value")
#             )

#             group = ids + list(choice) + [melt_col]

#             result = pd.concat([result, (
#                 df_melt
#                 # agrupa salas por filtros
#                 .groupby(group)
#                 .sum()
#                 .reset_index()
#                 .replace(melt)
#              )],axis=0)


#         if agg_col == "QT_MATRICULAS":

#             group = ids + list(choice)

#             result = pd.concat([result, (
#                  df
#                  # agrupa alunos por filtros
#                  .groupby(group)[agg_col]
#                  .sum()
#                  .reset_index()
#              )],axis=0)

#         if agg_col == "ID_DOCENTE":

#             group = ids + list(choice)
#             unique = cols + [agg_col]

#             result = pd.concat([result, (
#                  df
#                  # agrupa profs por filtros
#                  .drop_duplicates(subset=unique)
#                  .groupby(group)[agg_col]
#                  .count()
#                  .reset_index()
#              )],axis=0)

#     return result

# # Classroom data
# turmas = (
#     pd.read_csv('/Users/fernandascovino/Desktop/microdados_educacao_basica_2019/DADOS/TURMAS.CSV',
#                 encoding="latin-1", sep="|")
#     .pipe(treat_columns, replaces, levels)
# )
# test_turmas = turmas.pipe(agg_by_choice, 
#             ids=["CO_UF", "CO_MUNICIPIO", "TP_ETAPA_ANO"],
#             filters=["TP_DEPENDENCIA", "TP_LOCALIZACAO", "IN_AGUA_REDE_PUBLICA"],
#             agg_col="QT_MATRICULAS").fillna("Todos")

# # Schools data
# escolas = (pd.read_csv('/Users/fernandascovino/Desktop/microdados_educacao_basica_2019/DADOS/ESCOLAS.CSV',
#                      encoding="latin-1", sep="|")
#           .pipe(treat_columns, replaces, levels))

# melt = {"IN_COMUM_FUND_AI": "Fundamental - Anos iniciais",
#         "IN_COMUM_FUND_AF": "Fundamental - Anos finais",
#         "IN_COMUM_MEDIO_NORMAL": "Ensino Médio"}

# test_escolas = escolas.pipe(agg_by_choice, 
#             ids=["CO_UF", "CO_MUNICIPIO"],
#             filters=["TP_DEPENDENCIA", "TP_LOCALIZACAO", "IN_AGUA_REDE_PUBLICA"],
#             agg_col="QT_SALAS_UTILIZADAS",
#             melt=melt).fillna("Todos")

# Teachers data
# regioes = ["NORTE", "SUDESTE", "SUL", "NORDESTE", "CO"]

# profs = pd.DataFrame()

# for region in regioes:
#     path = f'/Users/fernandascovino/Desktop/microdados_educacao_basica_2019/DADOS/DOCENTES_{region}.CSV'
#     profs = profs.append(pd.read_csv(path, encoding="latin-1", sep="|")
#                 .pipe(treat_columns, replaces, levels))

# test_profs = profs.pipe(agg_by_choice, 
#             ids=["CO_UF", "CO_MUNICIPIO", "TP_ETAPA_ENSINO"],
#             filters=["TP_DEPENDENCIA", "TP_LOCALIZACAO", "IN_AGUA_REDE_PUBLICA"],
#             agg_col="ID_DOCENTE").fillna("Todos")

# Join tables
# def agg_by_level(df, levels,
#                  agg_col = "TP_ETAPA_ANO",
#                  new_col = "TP_ETAPA_ENSINO", 
#                  value = "QT_MATRICULAS"):
    
#     df[new_col] = df[agg_col].apply(lambda x: x[:-1]).map(levels)
#     cols = [col for col in df.columns if col not in [value]]
    
#     return df.groupby(group)[value].sum().reset_index()

# test_turmas_agg = test_turmas.pipe(agg_by_level, levels)
# test_turmas_agg.info()

# merge_cols = [
#     "CO_UF", "CO_MUNICIPIO","TP_DEPENDENCIA", "TP_LOCALIZACAO", "TP_ETAPA_ENSINO"
# ]
    
# df_full = test_turmas_agg.merge(test_escolas, on=merge_cols).merge(test_profs, on=merge_cols)

# df_full = df_full.rename(
#     columns={
#         "CO_UF": "state_num_id",
#         "CO_MUNICIPIO": "city_id",
#         "TP_DEPENDENCIA": "administrative_level",
#         "TP_LOCALIZACAO": "school_location",
#         "IN_AGUA_REDE_PUBLICA": "school_public_water_supply",
#         "TP_ETAPA_ENSINO": "education_phase",
#         "QT_MATRICULAS": "number_students",
#         "ID_DOCENTE": "number_teachers",
#         "QT_SALAS_UTILIZADAS": "number_classroms"
#     }
# )

# df_full.to_csv("br_cities_safeschools_censo_2019.csv")

# # Students by year data
# test_turmas_anos = test_turmas.rename(columns={
#         "CO_UF": "state_num_id",
#         "CO_MUNICIPIO": "city_id",
#         "TP_DEPENDENCIA": "administrative_level",
#         "TP_LOCALIZACAO": "school_location",
#         "TP_ETAPA_ENSINO": "education_phase",
#         "QT_MATRICULAS": "number_students",
#         "TP_ETAPA_ANO": "education_year"
#     })

# test_turmas_anos.to_csv("br_cities_safeschools_alunos_anos_censo_2019.csv")
