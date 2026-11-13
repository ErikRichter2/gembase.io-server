from server_core.commands.command_data import CommandData
from server_core.commands.commands_handler import CommandsHandler
from server_core.utils.utils import Utils
from src.session.users_session import UsersSession


billing_commands_handler = CommandsHandler(
    session_class=UsersSession
)

@billing_commands_handler.register("billing:get_modules_def")
def billing__get_modules_def(session: UsersSession):
    return session.user_db.billing_db.get_def()


@billing_commands_handler.register("billing:payment_request")
def billing__payment_request(session: UsersSession, data: CommandData):
    test_payment = False
    test_live_payment = False

    if session.user_db.is_admin():
        if "test_payment" in data.payload:
            test_payment = data.payload["test_payment"]
        if "test_live_payment" in data.payload:
            test_live_payment = data.payload["test_live_payment"]

    return session.user_db.billing_db.payment_request(
        billing_details=data.payload["billing_details"],
        modules_config=data.payload["modules_config"],
        test_payment=test_payment,
        test_live_payment=test_live_payment
    )

@billing_commands_handler.register("billing:payment_confirm")
def billing__payment_confirm(session: UsersSession, data: CommandData):
    request_guid = data.ensure('request_guid', Utils.is_guid)
    return session.user_db.billing_db.payment_confirm(
        request_guid=request_guid
    )

@billing_commands_handler.register("billing:get")
def billing__get_billings(session: UsersSession):
    return session.user_db.billing_db.get_billings()

@billing_commands_handler.register("billing:set_licences")
def billing__set_licences(session: UsersSession, data: CommandData):
    assert session.user_db.organization_db.is_organization_admin()
