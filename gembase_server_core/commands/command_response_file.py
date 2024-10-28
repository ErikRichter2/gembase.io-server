class CommandResponseFile:

    def __init__(self, payload: any, filename: str | None, mime: str | None):
        self.filename = filename
        self.mime = mime
        self.payload = payload
