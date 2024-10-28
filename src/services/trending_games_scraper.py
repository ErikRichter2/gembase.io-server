import src.services.trending_games.steamdb as steamdb
import src.services.trending_games.itch_io as itch_io
import src.services.trending_games.indie_db as indie_db
import src.services.trending_games.app_brain as app_brain
from gembase_server_core.db.db_connection import DbConnection


def process():

    print("Trending games START")

    conn = DbConnection()
    history_id = conn.insert("INSERT INTO trending_games.history (timestamp) VALUES (NOW())")

    steamdb.process(conn, history_id)
    itch_io.process(conn, history_id)
    indie_db.process(conn, history_id)
    app_brain.process(conn, history_id)

    conn.commit(True)

    print("Trending games END")


def default_method(*args, **kwargs):
    process()
