from typing import cast
from bs4 import Tag

from gembase_server_core.db.db_connection import DbConnection
from src.utils.gembase_utils import GembaseUtils
from src.utils.web import parse_web_page


def process(db: DbConnection, history_id: int):
    print("SteamDB START")
    soup = parse_web_page("https://steamdb.info")
    tables = soup.find_all("table", {"class": "table-products table-hover"})

    most_played = []
    trending = []
    popular = []
    hot_releases = []

    for table in tables:
        headers = table.find_all("th")
        for header in headers:
            a_tag = header.find("a")
            if a_tag is not None and 'Most Played' in a_tag.text:
                most_played = parse_most_played(table)
            if 'Trending' in header.text:
                trending = parse_trending(table)
            if a_tag is not None and 'Popular' in a_tag.text:
                popular = parse_popular(table)
            if a_tag is not None and 'Hot' in a_tag.text:
                hot_releases = parse_hot_releases(table)

    for it in most_played + trending + popular + hot_releases:
        if db.is_zero("SELECT * FROM trending_games.steamdb WHERE id = %s", [it['id']]):
            db.query("INSERT INTO trending_games.steamdb (id, name) VALUES (%s, %s)", [it['id'], it['name']])

    order = 1
    for it in most_played:
        db.query("INSERT INTO trending_games.steamdb_most_played (history_id, app_id, peak, players, order_col) VALUES (%s, %s, %s, %s, %s)",
                 [history_id, it['id'], it['peak'], it['players'], order])
        order += 1

    order = 1
    for it in trending:
        db.query("INSERT INTO trending_games.steamdb_trending (history_id, app_id, players, order_col) VALUES (%s, %s, %s, %s)",
                 [history_id, it['id'], it['players'], order])
        order += 1

    order = 1
    for it in popular:
        db.query("INSERT INTO trending_games.steamdb_popular (history_id, app_id, peak, price, order_col) VALUES (%s, %s, %s, %s, %s)",
                 [history_id, it['id'], it['peak'], it['price'], order])
        order += 1

    order = 1
    for it in hot_releases:
        db.query("INSERT INTO trending_games.steamdb_hot_releases (history_id, app_id, rating, price, order_col) VALUES (%s, %s, %s, %s, %s)",
                 [history_id, it['id'], it['rating'], it['price'], order])
        order += 1

    print("SteamDB END")

def parse_most_played(table: Tag):
    res = []
    tr_tags = table.find_all("tr", {"class": "app"})
    for tr in tr_tags:
        tag = cast(Tag, tr)
        if 'hidden' in tag.attrs:
            continue
        td_tags = tr.find_all("td")
        res.append({
            "id": int(tag.attrs['data-appid']),
            "name": td_tags[1].text.replace("\n", ""),
            "players": int(td_tags[2].text.replace(",", "")),
            "peak": int(td_tags[3].text.replace(",", "")),
        })
    return res


def parse_trending(table: Tag):
    res = []
    tr_tags = table.find_all("tr", {"class": "app"})
    for tr in tr_tags:
        tag = cast(Tag, tr)
        td_tags = tr.find_all("td")
        if len(td_tags) == 4:
            res.append({
                "id": int(tag.attrs['data-appid']),
                "name": td_tags[1].text.replace("\n", ""),
                "players": int(td_tags[3].text.replace(",", "")),
            })
    return res


def parse_popular(table: Tag):
    res = []
    tr_tags = table.find_all("tr", {"class": "app"})
    for tr in tr_tags:
        tag = cast(Tag, tr)
        td_tags = tr.find_all("td")
        res.append({
            "id": int(tag.attrs['data-appid']),
            "name": td_tags[1].text.replace("\n", ""),
            "peak": int(td_tags[2].text.replace(",", "")),
            "price": parse_price(td_tags[3].text),
        })
    return res


def parse_hot_releases(table: Tag):
    res = []
    tr_tags = table.find_all("tr", {"class": "app"})
    for tr in tr_tags:
        tag = cast(Tag, tr)
        td_tags = tr.find_all("td")
        res.append({
            "id": int(tag.attrs['data-appid']),
            "name": td_tags[1].text.replace("\n", ""),
            "rating": parse_rating(td_tags[2].text),
            "price": parse_price(td_tags[3].text),
        })
    return res


def parse_rating(rating: str) -> float:
    return GembaseUtils.float_safe(rating.replace("%", ""))


def parse_price(price: str) -> float:
    if price == 'Free':
        return 0
    return GembaseUtils.float_safe(price.replace("$", ""))
