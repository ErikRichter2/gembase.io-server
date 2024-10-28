from __future__ import annotations
from typing import TYPE_CHECKING

from src.session.session_factory import GbSessionFactory

if TYPE_CHECKING:
    from src.session.session_instance import GbSessionInstance


def gb_session() -> GbSessionInstance:
    return GbSessionFactory.get_or_create_session_from_flask_request(
        create_if_not_exists=True
    )


def is_logged():
    return gb_session().is_logged()


def is_admin():
    return gb_session().is_admin()


def is_user(user_id: int):
    return gb_session().user_id() == user_id
