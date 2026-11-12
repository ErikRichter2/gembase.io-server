from typing import Optional

from src.models.web_search.base_search import BaseSearch

class DemoSearch(BaseSearch):
    def search(self, query: str, silent=False) -> Optional[dict]:
        return None
