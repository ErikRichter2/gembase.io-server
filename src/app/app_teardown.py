from src.session.session_factory import GbSessionFactory


def teardown():
    session = GbSessionFactory.get_or_create_session_from_flask_request(
        create_if_not_exists=False
    )
    if session is not None:
        session.conn().close()
