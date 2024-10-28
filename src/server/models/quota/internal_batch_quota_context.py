from src.server.models.quota.user_quota_context import UserQuotaContext
from src.server.models.user.user_constants import uc


class InternalBatchQuotaContext(UserQuotaContext):

    def __init__(self, quota_type: str, context: str):
        super(InternalBatchQuotaContext, self).__init__(
            uc.get_system_batch_user_id(),
            quota_type,
            context)

    def has(self) -> bool:
        return True
