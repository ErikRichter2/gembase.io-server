from typing import Optional


class BaseSearch:
    
    def search(self, query: str, silent=False) -> Optional[dict]:
        raise Exception('Not implemented')
