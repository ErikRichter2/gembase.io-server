from dataclasses import dataclass
from typing import Optional


@dataclass
class ServerError:
    id: str
    message: str

class ServerException(Exception):

    def __init__(self, error: str | ServerError, error_code: int = 500, **kwargs):
        self.error_id = None
        self.message = None
        
        if isinstance(error, ServerError):
            self.error_id = error.id
            self.message = error.message
        else:
            self.message = error
        
        self.error_code = error_code
        
        if kwargs is not None:
            for arg in kwargs:
                self.message = self.message.replace(f"%{arg}%", kwargs[arg])

        super(ServerException, self).__init__(self.message)
