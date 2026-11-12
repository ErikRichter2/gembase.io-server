class CommandResponseFile:

    def __init__(self, payload: object, filename: str | None, mime: str | None):
        self.filename = filename
        self.mime = mime
        self.payload = payload