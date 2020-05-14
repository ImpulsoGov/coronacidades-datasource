import yaml
import requests
import os

def get_endpoints(url=os.getenv("ENDPOINTS_URL")):
    return yaml.load(requests.get(url).text, Loader=yaml.FullLoader)