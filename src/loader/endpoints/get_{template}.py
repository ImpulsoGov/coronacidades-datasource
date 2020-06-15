from endpoints.helpers import allow_local


@allow_local
def now(config):
    """This method is going to be called by main.py and it should return the output
    DataFrame.

    Parameters
    ----------
    config : dict
    """
    pass


# Output dataframe tests to check data integrity. This is also going to be called
# by main.py
TESTS = {
    "test1": lambda x: None,
    "test2": lambda x: None,
}
