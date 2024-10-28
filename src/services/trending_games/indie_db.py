from typing import cast
from bs4 import Tag

from gembase_server_core.db.db_connection import DbConnection
from src.utils.web import parse_web_page


def process(db: DbConnection, history_id: int):
    print("indie_db START")

    soup = parse_web_page("https://www.indiedb.com/games/top")

    top_games = soup.find_all("div", {"class": "rowcontent"})

    top_games_parsed = []

    for it in top_games:
        div = cast(Tag, it)
        a_tag = div.find("a")
        game_id = a_tag.attrs['href']
        div_content_tag = div.find("div", {"class": "content"})
        game_name = div_content_tag.find("h4").find("a").text
        game_url = game_id
        img_tag = a_tag.find("img")
        game_img = ""
        if img_tag is not None:
            game_img = img_tag.attrs['src']
        game_visits = div_content_tag.find("span", {"class": "date"}).find("span").attrs['title']
        time_tag = div_content_tag.find("span", {"class": "subheading"}).find("time")
        if 'datetime' in time_tag.attrs:
            game_datetime = time_tag.attrs['datetime']
        else:
            game_datetime = time_tag.text
        game_genre = div_content_tag.find("span", {"class": "subheading"}).text
        game_genre_time = div_content_tag.find("span", {"class": "subheading"}).find("time").text
        data = {
            "app_id": game_id,
            "name": game_name,
            "url": game_url,
            "img": game_img,
            "visits": parse_visits(game_visits),
            "time": game_datetime,
            "genre": parse_genre(game_genre, game_genre_time),
        }
        top_games_parsed.append(data)

    order = 1

    for it in top_games_parsed:
        row = db.select_one_or_none("""
        SELECT * FROM trending_games.indie_db WHERE app_id = %s
        """, [it['app_id']])
        if row is None:
            it['id'] = db.insert("""
            INSERT INTO trending_games.indie_db (app_id, name, img, url, time, genre) 
            VALUES (%s, %s, %s, %s, %s, %s)
            """, [it['app_id'], it['name'], it['img'], it['url'], it['time'], it['genre']])
        else:
            it['id'] = row['id']
            db.query("""
            UPDATE trending_games.indie_db SET name = %s, img = %s, url = %s, time = %s, genre = %s WHERE app_id = %s
            """, [it['name'], it['img'], it['url'], it['time'], it['genre'], it['id']])

        db.query("""
        INSERT INTO trending_games.indie_db_popular (history_id, app_id, visits, order_col) VALUES (%s, %s, %s, %s)
        """, [history_id, it['id'], it['visits'], order])
        order += 1

    print("indie_db END")


def parse_visits(visits: str) -> int:
    visits = visits.replace("visits today", "")
    return int(visits)


def parse_genre(genre: str, time: str) -> str:
    genre = genre.replace(time, "").replace("\n", "").replace("\t", "").strip()
    return genre

