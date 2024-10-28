class EmailData:
    def __init__(self):
        self.fromAddress: str = ""
        self.toAddress: str = ""
        self.subject: str = ""
        self.body: str = ""
        self.is_html: bool = False
