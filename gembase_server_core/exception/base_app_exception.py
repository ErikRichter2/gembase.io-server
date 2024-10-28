class BaseAppException(Exception):

    def __init__(self, module, error_id, messages, **kwargs):
        self.id = error_id
        self.module = module
        self.message = messages[error_id]
        self.error_code = 500
        self.is_public = False
        if kwargs is not None:
            for it in kwargs:
                self.message = self.message.replace(f"%{it}%", kwargs[it])
        super(BaseAppException, self).__init__(self.message)
