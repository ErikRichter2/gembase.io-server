from gembase_server_core.commands.command_data import CommandData


class FilesCommandData(CommandData):
    def __init__(self):
        self.files = []
        super(FilesCommandData, self).__init__()
