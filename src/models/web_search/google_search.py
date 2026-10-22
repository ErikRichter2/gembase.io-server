import json
from typing import Optional
from urllib.error import URLError
from urllib.parse import quote
from urllib.request import urlopen

from src.env_data import ENV_DATA
from src.models.web_search.base_search import BaseSearch

class GoogleSearch(BaseSearch):

    CUSTOM_SEARCH_URL: str | None = None
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    
    def search(self, query: str, silent=False) -> Optional[dict]:
        url = self.get_google_custom_search_url() + quote(query)
        try:
            fp = urlopen(url)
        except URLError as err:
            if silent:
                return None
            else:
                raise err
        content = fp.read().decode("utf8")
        return json.loads(content)


    def get_google_custom_search_url(self):
        global CUSTOM_SEARCH_URL
        if GoogleSearch.CUSTOM_SEARCH_URL is None:
            assert ENV_DATA.google_search_credentials is not None
            creds = json.loads(ENV_DATA.google_search_credentials)
            CUSTOM_SEARCH_URL = f"https://www.googleapis.com/customsearch/v1/siterestrict?key={creds['key']}&cx={creds['cx']}&q="
        return CUSTOM_SEARCH_URL