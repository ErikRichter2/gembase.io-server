class BaseQuotaContext:

    def __init__(self, audit_context):
        self.__audit_context = audit_context

    def has_enough_credits(self) -> bool:
        return False

    def pay_with_quota(self, count: int, audit_guid: str):
        pass

    def get_audit_context(self):
        return self.__audit_context