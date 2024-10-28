from typing import cast

from bs4 import Tag

from gembase_server_core.db.db_connection import DbConnection
from src.utils.web import parse_web_page


def process(db: DbConnection, history_id: int):
    print("AppBrain START")

    top_free = parse_ranking("https://webcache.googleusercontent.com/search?q=cache:https://www.appbrain.com/stats/google-play-rankings/top_free/game/us")
    top_paid = parse_ranking("https://webcache.googleusercontent.com/search?q=cache:https://www.appbrain.com/stats/google-play-rankings/top_paid/game/us")
    top_grossing = parse_ranking("https://webcache.googleusercontent.com/search?q=cache:https://www.appbrain.com/stats/google-play-rankings/top_grossing/game/us")
    top_new_free = parse_ranking("https://webcache.googleusercontent.com/search?q=cache:https://www.appbrain.com/stats/google-play-rankings/top_new_free/game/us")
    top_new_paid = parse_ranking("https://webcache.googleusercontent.com/search?q=cache:https://www.appbrain.com/stats/google-play-rankings/top_new_paid/game/us")

    def update_main(item):
        row = db.select_one_or_none("""
        SELECT * FROM trending_games.app_brain WHERE app_id = %s
        """, [item['app_id']])

        if row is None:
            item['id'] = db.insert("""
            INSERT INTO trending_games.app_brain (app_id, name, rating) 
            VALUES (%s, %s, %s)
            """, [item['app_id'], item['name'], item['rating']])
        else:
            item['id'] = row['id']
            db.query("""
            UPDATE trending_games.app_brain SET name = %s, rating = %s WHERE id = %s
            """, [item['name'], item['rating'], item['id']])

    order = 1
    for it in top_free:
        update_main(it)
        if db.is_zero("""
        SELECT * FROM trending_games.app_brain_top_free WHERE app_id = %s AND history_id = %s
        """, [it['id'], history_id]):
            db.query("""
            INSERT INTO trending_games.app_brain_top_free (history_id, app_id, order_col) VALUES (%s, %s, %s)
            """, [history_id, it['id'], order])
            order += 1

    order = 1
    for it in top_paid:
        update_main(it)
        if db.is_zero("""
        SELECT * FROM trending_games.app_brain_top_paid WHERE app_id = %s AND history_id = %s
        """, [it['id'], history_id]):
            db.query("""INSERT INTO trending_games.app_brain_top_paid (history_id, app_id, order_col) VALUES (%s, %s, %s)
            """, [history_id, it['id'], order])
            order += 1

    order = 1
    for it in top_grossing:
        update_main(it)
        if db.is_zero("""
        SELECT * FROM trending_games.app_brain_top_grossing WHERE app_id = %s AND history_id = %s
        """, [it['id'], history_id]):
            db.query("""
            INSERT INTO trending_games.app_brain_top_grossing (history_id, app_id, order_col) VALUES (%s, %s, %s)
            """, [history_id, it['id'], order])
            order += 1

    order = 1
    for it in top_new_paid:
        update_main(it)
        if db.is_zero("""
        SELECT * FROM trending_games.app_brain_top_new_paid WHERE app_id = %s AND history_id = %s
        """, [it['id'], history_id]):
            db.query("""
            INSERT INTO trending_games.app_brain_top_new_paid (history_id, app_id, order_col) VALUES (%s, %s, %s)
            """, [history_id, it['id'], order])
            order += 1

    order = 1
    for it in top_new_free:
        update_main(it)
        if db.is_zero("""
        SELECT * FROM trending_games.app_brain_top_new_free WHERE app_id = %s AND history_id = %s
        """, [it['id'], history_id]):
            db.query("""
            INSERT INTO trending_games.app_brain_top_new_free (history_id, app_id, order_col) VALUES (%s, %s, %s)
            """, [history_id, it['id'], order])
            order += 1

    print("AppBrain END")


def parse_ranking(url) -> []:
    res = []
    soup = parse_web_page(url)
    rankings_table = soup.find("table", {"id": "rankings-table"})
    if rankings_table is None:
        return []
    tr_tags = rankings_table.find_all("tr")
    game_name = ""
    game_rating = 0
    for tr_tag in tr_tags:
        game_id = ""
        tag = cast(Tag, tr_tag)
        td = tag.find("td", {"class": "ranking-app-cell"})
        if td is not None:
            a = td.find("a")
            game_id = a.attrs['href']
            index = game_id.rindex("/")
            game_id = game_id[(index+1):]
            game_name = a.text
        td = tag.find("td", {"class": "ranking-rating-cell"})
        if td is not None:
            span = td.find("span", recursive=False)
            game_rating = span.text

        if game_id != "":
            res.append({
                "app_id": game_id,
                "name": game_name,
                "rating": game_rating,
            })
    return res
