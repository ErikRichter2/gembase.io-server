from gembase_server_core.exception.base_app_exception import BaseAppException


class AuthException(BaseAppException):
    AUTH001 = "AUTH001"
    AUTH002 = "AUTH002"
    AUTH003 = "AUTH003"
    AUTH004 = "AUTH004"
    AUTH005 = "AUTH005"
    AUTH006 = "AUTH006"
    AUTH007 = "AUTH007"
    AUTH999 = "AUTH999"

    ERROR_MESSAGES = {
        AUTH001: "Invalid token !",
        AUTH002: "Invalid email or password !",
        AUTH003: "Session expired ! Please login again.",
        AUTH004: "Unauthorized access",
        AUTH005: "Email is already registered !",
        AUTH006: "Your email address is not whitelisted - please contact contact@gembase.io !",
        AUTH007: "User not found !",
        AUTH999: "Unexpected error ! Please contact gembase.io support.",
    }

    def __init__(self, error_id, **kwargs):
        super(AuthException, self).__init__("auth", error_id, AuthException.ERROR_MESSAGES, **kwargs)
        self.error_code = 401
        self.is_public = True
