import pandas as pd

from endpoints import get_cities_farolcovid_main, get_states_farolcovid_main
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

    # TODO: update on config!
    df = (
        download_from_drive(
            "https://docs.google.com/spreadsheets/d/1sNxh1VOWyOPXG4lfpkRJGnLbbhdBgKQnWdk3PRVhvMM"
        )
        .assign(
            city_id=lambda df: df["city_id"].astype(str),
            state_num_id=lambda df: df["state_num_id"].astype(int),
        )
        .merge(
            pd.concat(
                [
                    get_states_farolcovid_main.now(config)[
                        [
                            "state_num_id",
                            "state_id",
                            "overall_alert",
                            "last_updated_cases",
                        ]
                    ].assign(city_id=lambda df: "Todos", city_name=lambda df: "Todos"),
                    get_cities_farolcovid_main.now(config)[
                        [
                            "state_num_id",
                            "state_id",
                            "city_id",
                            "city_name",
                            "overall_alert",
                            "last_updated_cases",
                        ]
                    ].assign(city_id=lambda df: df["city_id"].astype(str)),
                ]
            ),
            on=["city_id", "state_num_id"],
            how="left",
        )
    )

    return df


# Output dataframe tests to check data integrity. This is also going to be called
# by main.py
TESTS = {
    "more than 5570 cities": lambda df: len(df["city_id"].unique()) != 5571,
    "data is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
    "dataframe has null data": lambda df: all(
        df.drop(columns=["overall_alert", "last_updated_cases"]).isnull().any() == False
    ),
    "sum of classrooms for all administrative levels is not equal to total classrooms": lambda df: all(
        df[df["administrative_level"] == "Municipal"]
        .groupby("education_phase")["number_classroms"]
        .sum()
        + df[df["administrative_level"] == "Estadual"]
        .groupby("education_phase")["number_classroms"]
        .sum()
        == df[df["administrative_level"] == "Todos"]
        .groupby("education_phase")["number_classroms"]
        .sum()
    ),
    "sum of classrooms for all school locations is not equal to total classrooms": lambda df: all(
        df[df["school_location"] == "Rural"]
        .groupby("education_phase")["number_classroms"]
        .sum()
        + df[df["school_location"] == "Urbana"]
        .groupby("education_phase")["number_classroms"]
        .sum()
        == df[df["school_location"] == "Todos"]
        .groupby("education_phase")["number_classroms"]
        .sum()
    ),
    "sum of students for all administrative levels is not equal to total students": lambda df: all(
        df[df["administrative_level"] == "Municipal"]
        .groupby("education_phase")["number_students"]
        .sum()
        + df[df["administrative_level"] == "Estadual"]
        .groupby("education_phase")["number_students"]
        .sum()
        == df[df["administrative_level"] == "Todos"]
        .groupby("education_phase")["number_students"]
        .sum()
    ),
    "sum of teachers for all administrative levels is not equal to total teachers": lambda df: all(
        df[df["administrative_level"] == "Municipal"]
        .groupby("education_phase")["number_teachers"]
        .sum()
        + df[df["administrative_level"] == "Estadual"]
        .groupby("education_phase")["number_teachers"]
        .sum()
        >= df[df["administrative_level"] == "Todos"]
        .groupby("education_phase")["number_teachers"]
        .sum()
    ),
    "sum of teachers for all cities is not equal to total teachers": lambda df: all(
        df[df["city_id"] != "Todos"].groupby("education_phase")["number_teachers"].sum()
        >= df[df["city_id"] == "Todos"]
        .groupby("education_phase")["number_teachers"]
        .sum()
    ),
}

# ==> Code from original School Census data:
# replaces = {
#     "IN_ESPECIAL_EXCLUSIVA": {
#         0: "Não exclusiva"
#     },
#     "TP_SITUACAO_FUNCIONAMENTO": {
#         1: "Ativa"
#     },
#     "TP_TIPO_ATENDIMENTO_TURMA": {
#         1: "Escolarização/Complementar",
#         2: "Escolarização/Complementar",
#         3: "Escolarização/Complementar",
#     },
#     "TP_TIPO_DOCENTE": {
#         1: "Docente/Monitor",
#         3: "Docente/Monitor"
#     },
#     "TP_ETAPA_ENSINO": {
#         14: "Fundamental - Anos iniciais",
#         15: "Fundamental - Anos iniciais",
#         16: "Fundamental - Anos iniciais",
#         17: "Fundamental - Anos iniciais",
#         18: "Fundamental - Anos iniciais",
#         19: "Fundamental - Anos finais",
#         20: "Fundamental - Anos finais",
#         21: "Fundamental - Anos finais",
#         41: "Fundamental - Anos finais",
#         25: "Ensino Médio",
#         26: "Ensino Médio",
#         27: "Ensino Médio",
#         28: "Ensino Médio",
#     }, # 56: "Fundamental - Anos iniciais", multi-etapa?
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

# def melt_indicators(df, to_melt, keep_vars, new_col):
#     """
#     Transforma um conjunto de indicadores numa nova coluna.

#     to_melt:
#         Dicionário contendo [nome_indicador]: [valor_na_nova_coluna]
#         (ex: {"IN_COMUM_FUND_AI": "Fundamental - Anos iniciais"})
#     id_vars:
#         Lista de variáveis para se manter no dataframe
#     new_col:
#         Nome da nova coluna que irá conter os valores passados em `to_melt`.
#     """

#     return (
#         df[keep_vars + list(to_melt.keys())]
#         .melt(id_vars=keep_vars,
#               var_name=[new_col],
#               value_name='value')
#         .query("value == 1")
#         .drop(columns="value")
#         .replace(to_melt)
#     )

# def treat_data(df, replaces, value):
#     for col in replaces:
#         if col in df.columns:
#             # Filtra somente por valoresno dicionario
#             df = df.query(f"{col} in {list(replaces[col].keys())}")
#             # Substitui códigos
#             df[col] = df[col].replace(replaces[col])

#     keep = ["CO_UF", "CO_MUNICIPIO", "CO_ENTIDADE"] + [col for col in replaces.keys() if col in df.columns]

#     # Escolas --> Cria coluna TP_ETAPA_ENSINO
#     if value == "QT_SALAS_UTILIZADAS":
#         to_melt = {"IN_COMUM_FUND_AI": "Fundamental - Anos iniciais",
#                    "IN_COMUM_FUND_AF": "Fundamental - Anos finais",
#                    "IN_COMUM_MEDIO_NORMAL": "Ensino Médio"}

#         new_col="TP_ETAPA_ENSINO"

#         df = melt_indicators(df, to_melt, keep + [value], new_col)
#         keep +=[new_col]

#     # Profs --> Remove duplicacao por escola
#     if value == "ID_DOCENTE":
#         df = df.drop_duplicates(subset=keep+[value])

#     agg_func = {
#         "QT_MATRICULAS": "sum",
#         "QT_SALAS_UTILIZADAS": "sum",
#         "ID_DOCENTE": "count"}

#     return df.groupby(keep)[value].agg(agg_func[value]).reset_index()

# turmas = (
#     pd.read_csv('/Users/fernandascovino/Desktop/microdados_educacao_basica_2019/DADOS/TURMAS.CSV',
#                 encoding="latin-1", sep="|")
#     .pipe(treat_data, replaces, value="QT_MATRICULAS")
# )

# escolas = (pd.read_csv('/Users/fernandascovino/Desktop/microdados_educacao_basica_2019/DADOS/ESCOLAS.CSV',
#                      encoding="latin-1", sep="|")
#           .pipe(treat_data, replaces, value="QT_SALAS_UTILIZADAS", data_type="escolas"))

# profs = pd.DataFrame()

# for region in ["NORTE", "SUDESTE", "SUL", "NORDESTE", "CO"]:
#     path = f'/Users/fernandascovino/Desktop/microdados_educacao_basica_2019/DADOS/DOCENTES_{region}.CSV'
#     profs = profs.append(pd.read_csv(path, encoding="latin-1", sep="|")
#                 .pipe(treat_data, replaces, value="ID_DOCENTE"))

# # Junta tabelas
# aux = profs.merge(turmas, on=list(set(profs.columns) & set(turmas.columns)))
# aux = (
#     aux
#        .merge(escolas, on=list(set(aux.columns) & set(escolas.columns)))
#        .assign(CO_MUNICIPIO= lambda df: df.CO_MUNICIPIO.astype(int).astype(str))
# )

# # Calcula permutacoes dos filtros
# from itertools import chain, combinations

# def all_filters_choices(filters):
#     "powerset([1,2,3]) --> () (1,) (2,) (3,) (1,2) (1,3) (2,3) (1,2,3)"
#     s = list(filters)
#     return list(chain.from_iterable(combinations(s, r) for r in range(len(s)+1)))

# def agg_by_choice(df, ids, filters, agg_cols):
#     """
#     Agrega dados de uma coluna por id e todas as combinações possíveis de filtros.
#     """

#     # Cria todas as permutações de filtros
#     all_filters = all_filters_choices([col for col in filters if col in df.columns])

#     result = pd.DataFrame()
#     for choice in all_filters:

#         group = ids + list(choice)
#         result = pd.concat([result, (
#              df
#              .groupby(group)[agg_cols]
#              .sum()
#              .reset_index()
#          )],axis=0)

#     return result

# df_full = agg_by_choice(aux,
#               ids=["CO_UF", "TP_ETAPA_ENSINO"],
#               filters=['CO_MUNICIPIO', 'TP_DEPENDENCIA', 'TP_LOCALIZACAO', 'IN_AGUA_REDE_PUBLICA'],
#               agg_cols = [
#                 "QT_MATRICULAS",
#                 "QT_SALAS_UTILIZADAS",
#                 "ID_DOCENTE"]).fillna("Todos")

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
