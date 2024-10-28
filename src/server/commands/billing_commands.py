from gembase_server_core.commands.command_data import CommandData
from gembase_server_core.commands.command_decorator import command
from src.session.session import gb_session, is_logged
from src.utils.gembase_utils import GembaseUtils


@command("portal:billing:get_modules_def", [is_logged])
def billing__get_modules_def():
    return gb_session().models().billing().get_def()


@command("portal:billing:payment_request", [is_logged])
def billing__payment_request(data: CommandData):
    test_payment = False
    test_live_payment = False

    if gb_session().is_admin():
        if "test_payment" in data.payload:
            test_payment = data.payload["test_payment"]
        if "test_live_payment" in data.payload:
            test_live_payment = data.payload["test_live_payment"]

    return gb_session().models().billing().payment_request(
        billing_details=data.payload["billing_details"],
        modules_config=data.payload["modules_config"],
        test_payment=test_payment,
        test_live_payment=test_live_payment
    )


@command("portal:billing:payment_confirm", [is_logged])
def billing__payment_confirm(data: CommandData):
    assert GembaseUtils.is_guid(data.payload["request_guid"])

    return gb_session().models().billing().payment_confirm(
        request_guid=data.payload["request_guid"]
    )


@command("portal:billing:get_billings", [is_logged])
def billing__get_billings():
    return gb_session().models().billing().get_billings()


@command("portal:billing:send_confirmation_mail", [is_logged])
def billing__send_confirmation_mail(data: CommandData):
    assert GembaseUtils.is_guid(data.payload["guid"])
    assert gb_session().user().get_organization().is_organization_admin()

    return gb_session().user().get_organization().send_confirmation_mail(
        request_guid=data.payload["guid"]
    )


@command("portal:billing:set_organization_request", [is_logged])
def billing__set_organization_request(data: CommandData):
    assert gb_session().user().get_organization().is_organization_admin()

    guid = None

    if data.payload["action"] == "insert":
        assert GembaseUtils.is_email(data.payload["email"])
        guid = gb_session().user().get_organization().add_request(
            email=data.payload["email"]
        )
    elif data.payload["action"] == "delete":
        assert GembaseUtils.is_guid(data.payload["guid"])
        guid = data.payload["guid"]
        gb_session().user().get_organization().remove_request(
            request_guid=data.payload["guid"]
        )
    return guid


@command("portal:billing:set_licences", [is_logged])
def billing__set_licences(data: CommandData):
    assert gb_session().user().get_organization().is_organization_admin()

    gb_session().user().get_organization().set_licences(
        added_accounts=data.payload["added_accounts"],
        licences=data.payload["licences"]
    )
