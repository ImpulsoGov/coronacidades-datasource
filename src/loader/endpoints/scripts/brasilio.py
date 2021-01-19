import json
from urllib.parse import urljoin
from urllib.request import Request, urlopen

class BrasilIO:

    base_url = "https://api.brasil.io/v1/"

    def __init__(self, user_agent=None, auth_token=None):
        """
        Caso queria fazer uma requisição na API, passe os parâmetros user_agent e auth_token.
        Para fazer somente o download do arquivo completo, não é necessário passar nenhum parâmetro.
        """
        self.__user_agent = user_agent
        self.__auth_token = auth_token

    def headers(self, api=True):
        if api:
            return {
                "User-Agent": f"{self.__user_agent}",
                "Authorization": f"Token {self.__auth_token}"
            }
        else:
            return {
                "User-Agent": "python-urllib/brasilio-client-0.1.0",
            }
            

    def api_request(self, path, query_string=None):
        url = urljoin(self.base_url, path)

        if query_string:
            url += "?" + query_string

        request = Request(url, headers=self.headers(api=True))
        response = urlopen(request)
        
        return json.load(response)
        
    def data(self, dataset_slug, table_name, filters=None):
        url = f"dataset/{dataset_slug}/{table_name}/data/"
        filters = filters or {}
        filters["page"] = 1

        finished = False
        
        results = []
        while not finished:
            query_string = "&".join([f"{k}={v}" for k, v in filters.items()])
            response = self.api_request(url, query_string)
            results += response["results"]
            next_page = response.get("next", None)
            filters = {}
            url = next_page
            finished = next_page is None

    def download(self, dataset, table_name):
        url = f"https://data.brasil.io/dataset/{dataset}/{table_name}.csv.gz"
        request = Request(url, headers=self.headers(api=False))
        response = urlopen(request)
        return response
