class BaseQuotaContext:

    def __init__(self, audit_context):
        self.__audit_context = audit_context

    def has(self) -> bool:
        return False

    def add(self, count: int, audit_guid: str):
        pass

    def get_audit_context(self):
        return self.__audit_context
