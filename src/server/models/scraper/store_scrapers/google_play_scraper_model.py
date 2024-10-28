import json
from datetime import datetime

from bs4 import BeautifulSoup
from google_play_scraper.constants.request import Formats
from google_play_scraper.features.app import parse_dom

from src.server.models.scraper.scraper_utils import ScraperUtils
from src.utils.gembase_utils import GembaseUtils
from src.utils.web import WebParseUtils


class GooglePlayScraperModel:

    @staticmethod
    def __get_dev_apps(page_content: str) -> [str]:
        token = 'href="/store/apps/details?id='
        idx = 0
        app_ids = []

        while True:
            i1 = page_content.find(token, idx)
            if i1 == -1:
                break
            i1 += len(token)
            i2 = page_content.find('"', i1)
            if i2 == -1:
                break
            app_id = page_content[i1: i2]
            if app_id not in app_ids:
                app_ids.append(app_id)
            idx = i2

        return app_ids

    @staticmethod
    def get_dev_id_from_dev_id_in_store(dev_id_in_store: str) -> str:
        dev_id = dev_id_in_store
        dev_id = dev_id.replace("dev?id=", "")
        dev_id = dev_id.replace("developer?id=", "")
        return dev_id

    @staticmethod
    def scrap_dev(dev_id_in_store: str):
        base_url = "https://play.google.com/store/apps/"

        page_content = None
        url = f"{base_url}{dev_id_in_store}"

        if "dev?id=" not in dev_id_in_store and "developer?id=" not in dev_id_in_store:
            url = f"{base_url}dev?id={dev_id_in_store}"
            page_content = GembaseUtils.load_page(url)
            if page_content is None or "<title>Not Found</title>" in page_content:
                dev_id_in_store = f"developer?id={dev_id_in_store}"
                page_content = None
                url = f"{base_url}{dev_id_in_store}"
            else:
                dev_id_in_store = f"dev?id={dev_id_in_store}"

        if page_content is None:
            page_content = GembaseUtils.load_page(url)

        if page_content is None or "<title>Not Found</title>" in page_content:
            return {
                "state": -1
            }

        ix1 = page_content.find("og:title")
        ix2 = page_content.find('content="', ix1)
        ix3 = page_content.find('">', ix1)
        dev_name = page_content[ix2:ix3]
        dev_name = dev_name.replace('content="', "").replace("Android Apps by ", "").replace(" on Google Play", "")

        dev_apps = GooglePlayScraperModel.__get_dev_apps(page_content)

        # add apps from US store
        page_content_us = GembaseUtils.load_page(f"{url}&gl=US")
        if page_content_us is not None:
            dev_apps_us = GooglePlayScraperModel.__get_dev_apps(page_content_us)
            for app_id in dev_apps_us:
                if app_id not in dev_apps:
                    dev_apps.append(app_id)

        return {
            "state": 1,
            "title": dev_name,
            "source_data": {
                "store_page": page_content
            },
            "dev_apps": dev_apps,
            "dev_id": GooglePlayScraperModel.get_dev_id_from_dev_id_in_store(dev_id_in_store),
            "dev_id_in_store": dev_id_in_store,
            "url": url
        }

    @staticmethod
    def scrap_app(app_id: str):
        url = Formats.Detail.build(app_id=app_id, lang='en', country='us')

        page_content = None
        try:
           page_content = WebParseUtils.get_page_urlopen(url)
        except:
            pass

        if page_content is None:
            url = Formats.Detail.fallback_build(app_id=app_id, lang='en')
            try:
                page_content = WebParseUtils.get_page_urlopen(url)
            except:
                pass

        if page_content is None:
            return {
                "state": -1,
                "state_str": "Store page not found"
            }

        scrapped_data = parse_dom(dom=page_content, app_id=app_id, url=url)

        bs = BeautifulSoup(page_content, 'html.parser')

        div = bs.find("div", {"data-g-id": "description"})
        if div is None:
            return {
                "state": -2,
                "state_str": "div data-g-id not found"
            }
        divs = div.find_next_siblings("div")
        store_tags = []
        store_tags_ids = []
        genres = []
        tags = []
        for div in divs:
            a_arr = div.find_all("a")
            if a_arr is not None:
                for a in a_arr:
                    if a is not None:
                        href = a.get("href")
                        if href is not None:
                            if '/store/apps/category/' in href:
                                genre_id: str = href.replace('/store/apps/category/', '')
                                store_tag_id = genre_id.strip().lower()
                                if store_tag_id not in store_tags_ids:
                                    store_tags_ids.append(store_tag_id)
                                    store_tags.append(genre_id)
                                    genres.append(genre_id)
                            elif '/store/search?q=' in href:
                                tag: str = href.replace('/store/search?q=', '').replace('&c=apps', '')
                                store_tag_id = tag.strip().lower()
                                if store_tag_id not in store_tags_ids:
                                    store_tags_ids.append(store_tag_id)
                                    store_tags.append(tag)
                                    tags.append(tag)

        scrapped_data['genres'] = genres
        scrapped_data['tags'] = tags

        released = scrapped_data['released']
        scrapped_data['released'] = None
        scrapped_data['updated'] = None
        if released is not None:
            released = released.replace('Jan', '01').replace('Feb', '02').replace('Mar', '03').replace('Apr', '04')
            released = released.replace('May', '05').replace('Jun', '06').replace('Jul', '07').replace('Aug', '08')
            released = released.replace('Sep', '09').replace('Oct', '10').replace('Nov', '11').replace('Dec', '12')
            vals = released.split(', ')
            vals1 = vals[0].split(' ')
            month = int(vals1[0])
            day = int(vals1[1])
            year = int(vals[1])
            scrapped_data['released'] = datetime(year, month, day).timestamp()
        if scrapped_data['updated'] is not None:
            updated_dt = datetime.fromtimestamp(scrapped_data['updated'])
            scrapped_data['updated'] = updated_dt.timestamp()

        rating = 0
        if scrapped_data["score"] is not None:
            rating = round(scrapped_data["score"] / 5 * 100)

        ratings = 0
        if "ratings" in scrapped_data and scrapped_data["ratings"] is not None:
            ratings = scrapped_data["ratings"]

        reviews = 0
        if "reviews" in scrapped_data and scrapped_data["reviews"] is not None:
            reviews = scrapped_data["reviews"]

        gallery = []
        if "screenshots" in scrapped_data:
            for screenshot in scrapped_data["screenshots"]:
                gallery.append(screenshot)

        price = 0
        if "price" in scrapped_data and scrapped_data["price"] is not None:
            price = int(scrapped_data["price"])

        initial_price = price
        if "originalPrice" in scrapped_data and scrapped_data["originalPrice"] is not None:
            initial_price = int(scrapped_data["originalPrice"])

        dev_ids = [
            {
                "dev_id": scrapped_data["developerId"],
                "dev_id_in_store": scrapped_data["developerId"]
            }
        ]

        icon_bytes = ScraperUtils.get_app_icon_bytes(scrapped_data["icon"])

        ads = False
        if "Contains ads" in page_content:
            ads = True

        iap = False
        if "In-app purchases" in page_content:
            iap = True

        return {
            "state": 1,
            "reviews": reviews,
            "ratings": ratings,
            "dev_ids": dev_ids,
            "title": scrapped_data["title"],
            "icon": scrapped_data["icon"],
            "description": scrapped_data["description"],
            "rank_val": scrapped_data["realInstalls"],
            "installs": scrapped_data["realInstalls"],
            "released": scrapped_data['released'],
            "rating": rating,
            "source_data": {
                "store_page": page_content,
                "scrap_data": json.dumps(scrapped_data)
            },
            "gallery": gallery,
            "price": price,
            "initial_price": initial_price,
            "url": url,
            "store_tags": store_tags,
            "icon_bytes": icon_bytes,
            "ads": ads,
            "iap": iap
        }
