class ServiceData:

    def __init__(self, service_id: int | None = None, service_guid: str | None = None):
        self.parent: ServiceData | None = None
        self.service_id: int = service_id
        self.service_guid: str = service_guid
        self.cancel: bool = False
        self.input_data: any = {}
        self.result_data: any = None
        self.verbose_data: any = None
        self.__set_state_callback = None
        self.__set_verbose_data_callback = None
        self.__check_cancel_callback = None

    def set_state_callback(self, callback):
        self.__set_state_callback = callback

    def set_check_cancel_callback(self, callback):
        self.__check_cancel_callback = callback

    def set_result_data(self, result_data: any):
        self.result_data = result_data
        if self.__set_state_callback is not None:
            self.__set_state_callback(self)

    def clear(self):
        self.cancel = False
        self.input_data = {}
        self.result_data = None
        self.verbose_data = None

    def check_cancel(self) -> bool:
        if self.cancel:
            return True
        if self.__check_cancel_callback is not None:
            return self.__check_cancel_callback(self)
        if self.parent is not None:
            return self.parent.check_cancel()
        return False

    def set_verbose_data(self, verbose_data):
        self.verbose_data = verbose_data
        if self.__set_verbose_data_callback is not None:
            self.__set_verbose_data_callback(self)

    def set_verbose_data_callback(self, callback):
        self.__set_verbose_data_callback = callback
