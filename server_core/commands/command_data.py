from types import FunctionType
from typing import Any, Generic, Optional, TypeVar, overload

from server_core.exception.server_exception import ServerException

T = TypeVar('T')

class CommandData:
    def __init__(self):
        self.id: str = ""
        self.payload: dict = {}
        self.metadata = None
        
    def ensure(self, key: str, assert_fn: Optional[FunctionType] = None):
        if key not in self.payload:
            raise ServerException(f'CommandData.get: {key} not found')
        
        v = self.payload[key]
        if assert_fn is not None:
            if not assert_fn(v):
                raise ServerException(f'CommandData.get: {key} assert {str(assert_fn)}')
        return v

    @overload
    def try_ensure(self, key: str, assert_fn: Optional[FunctionType], default_value: T) -> T: ...

    @overload
    def try_ensure(self, key: str, assert_fn: Optional[FunctionType] = None, default_value: None = None) -> Any: ...

    def try_ensure(self, key: str, assert_fn: Optional[FunctionType] = None, default_value: Optional[T] = None):
        if key not in self.payload:
            return default_value
        return self.ensure(key=key, assert_fn=assert_fn)

    def get_metadata(self, key: str):
        if self.metadata is None:
            return None
        if key not in self.metadata:
            return None
        return self.metadata[key]
