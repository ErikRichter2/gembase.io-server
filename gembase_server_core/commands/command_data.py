class CommandData:
    def __init__(self):
        self.id: str = ""
        self.payload: dict = {}

    def get(self, key: str, default_value=None):
        if key in self.payload:
            return self.payload[key]
        return default_value
