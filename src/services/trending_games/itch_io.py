from typing import cast
from bs4 import Tag

from gembase_server_core.db.db_connection import DbConnection
from src.utils.gembase_utils import GembaseUtils
from src.utils.web import parse_web_page


def process(db: DbConnection, history_id: int):
    print("itch.io START")

    soup = parse_web_page("https://itch.io")

    featured_games_grid = soup.find("div", {"class": "featured_game_grid_widget"})
    game_cells = featured_games_grid.find_all("div", {"class": "game_cell"})

    featured = []

    for it in game_cells:
        div = cast(Tag, it)
        game_id = div.attrs['data-game_id']
        a_game_thumb = div.find("a", {"class": "game_thumb"})
        game_url = a_game_thumb.attrs['href']
        img = a_game_thumb.find("img")
        game_img = img.attrs['data-lazy_src']
        div_label = div.find("a", {"class": "title"})
        game_name = div_label.text
        game_tags = []
        div_tags = div.find("div", {"class": "sub cell_tags"})
        meta_tag = div.find("div", {"class": "meta_tag"})
        game_price = 0
        if meta_tag is not None:
            game_price = meta_tag.text
        if div_tags is not None:
            a_tags = div_tags.find_all("a")
            for a_game_tag in a_tags:
                game_tags.append(a_game_tag.text)
            div_desc = div_tags.find_next_sibling("div", {"class": "sub"})
            game_desc = div_desc.text
        else:
            div_desc = div.find("div", {"class": "sub"})
            game_desc = div_desc.text
        data = {
            "id": int(game_id),
            "url": game_url,
            "img": game_img,
            "name": game_name,
            "tags": ','.join(game_tags),
            "desc": game_desc,
            "price": parse_price(game_price),
            "is_web": is_web(game_price),
        }
        featured.append(data)

    order = 1
    for it in featured:
        if db.is_zero("SELECT * FROM trending_games.itch_io WHERE id = %s", [it['id']]):
            db.query("""
            INSERT INTO trending_games.itch_io (id, name, tags, game_desc, url, img, is_web) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                     [it['id'], it['name'], it['tags'], it['desc'], it['url'], it['img'], it['is_web']])
        else:
            db.query("""
            UPDATE trending_games.itch_io 
               SET name = %s, tags = %s, game_desc = %s, url = %s, img = %s, is_web = %s 
             WHERE id = %s
            """, [it['name'], it['tags'], it['desc'], it['url'], it['img'], it['is_web'], it['id']])

        db.query("""
        INSERT INTO trending_games.itch_io_featured (history_id, app_id, price, order_col) 
        VALUES (%s, %s, %s, %s)
        """, [history_id, it['id'], it['price'], order])
        order += 1

    print("itch.io END")


def parse_price(price: str) -> float:
    if is_web(price) or price == 'Free':
        return 0
    return GembaseUtils.float_safe(price.replace("$", "").replace("€", ""))


def is_web(price: str) -> bool:
    return price == 'Web'

