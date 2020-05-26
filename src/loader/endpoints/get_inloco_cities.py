import pandas as pd
from utils import secrets
from endpoints.helpers import allow_local
import endpoints.GoogleDocsLib as gd


@allow_local
def now(config):
    file_id = secrets(["inloco", "cities", "id"])
    return gd.downloadGoogleFileDF(file_id, "token.pickle")


TESTS = {
    "data is not pd.DataFrame": lambda df: isinstance(df, pd.DataFrame),
}
