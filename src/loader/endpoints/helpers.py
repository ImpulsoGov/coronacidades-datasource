import pandas as pd

from utils import build_file_path, get_endpoints


def allow_local(func):
    def wrapper(*args, **kwargs):

        if kwargs.get("force"):

            kwargs.pop("force")

            return func(*args, **kwargs)

        else:

            module = func.__module__.split(".")[1]

            endpoint = [l for l in get_endpoints() if module in l.values()][0]

            return pd.read_csv(build_file_path(endpoint))

    return wrapper
