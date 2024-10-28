import json
from json import JSONDecodeError
from time import sleep
from datetime import datetime

from src.server.models.scraper.scraper_utils import ScraperUtils
from src.utils.gembase_utils import GembaseUtils
from src.utils.web import WebParseUtils

release_date_token = '<div class="date">'
months = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]


class SteamScraperModel:

    @staticmethod
    def get_dev_id_from_dev_id_in_store(dev_id_in_store: str) -> str:
        dev_id = dev_id_in_store

        if "search/" in dev_id_in_store:
            ix = dev_id_in_store.find("=")
            dev_id = dev_id_in_store[(ix + 1):]
        elif "/" in dev_id_in_store:
            ix = dev_id_in_store.find("/")
            dev_id = dev_id_in_store[(ix + 1):]

        dev_id = SteamScraperModel.get_app_id_from_steam_id(SteamScraperModel.get_steam_id_from_app_id(dev_id))
        dev_id = dev_id.lower()

        return dev_id

    @staticmethod
    def get_app_id_from_steam_id(steam_id: str) -> str:
        return f"steam__{steam_id}"

    @staticmethod
    def get_steam_id_from_app_id(app_id: str) -> str:
        return app_id.replace("steam__", "")

    @staticmethod
    def __get_dev_top_50_apps(dev_id_in_store: str) -> []:
        app_ids = []
        if "search/?" in dev_id_in_store:
            tmp = dev_id_in_store.replace("search/?", "")
            url = f"https://store.steampowered.com/search/results?force_infinite=1&{tmp}&tags=&ndl=1&snr="
            res = WebParseUtils.get_page_urlopen(url)
            if res is not None:
                ix = 0
                s = "https://store.steampowered.com/app/"
                while ix != -1:
                    ix = res.find(s, ix)
                    if ix > 0:
                        ix += len(s)
                        ix2 = res.find("/", ix)
                        ix3 = res.find('"', ix)
                        app_id = res[ix:min(ix2, ix3)]
                        app_id = SteamScraperModel.get_app_id_from_steam_id(app_id)
                        if app_id not in app_ids:
                            app_ids.append(app_id)
        else:
            url = f"https://store.steampowered.com/{dev_id_in_store}/ajaxgetfilteredrecommendations/render/?query=&start=0&count=50&tagids=&sort=topsellers&app_types=&curations=&reset=true"

            res = WebParseUtils.get_page_urlopen(url)
            if res is not None:
                try:
                    res_data = json.loads(res)
                except JSONDecodeError:
                    return []
                if "success" in res_data and res_data["success"] == 1:
                    html_data: str = res_data["results_html"]
                    si = 0
                    app_id_data_name = "data-ds-appid"
                    while True:
                        try:
                            i = html_data.index(app_id_data_name, si)
                            i1 = html_data.index('"', i)
                            i2 = html_data.index('"', i1 + 1)
                            si = i2
                            app_id = SteamScraperModel.get_app_id_from_steam_id(html_data[i1 + 1: i2])
                            if app_id not in app_ids:
                                app_ids.append(app_id)
                        except Exception:
                            break

        return app_ids

    @staticmethod
    def scrap_dev(dev_id_in_store: str):
        if "steam__" in dev_id_in_store:
            dev_id_in_store = dev_id_in_store.replace("steam__", "")
            dev_id_in_store = f"developer/{dev_id_in_store}"

        url = f"https://store.steampowered.com/{dev_id_in_store}"
        store_page = GembaseUtils.load_page(url)
        sleep(1)
        dev_apps = SteamScraperModel.__get_dev_top_50_apps(dev_id_in_store=dev_id_in_store)

        dev_id = SteamScraperModel.get_dev_id_from_dev_id_in_store(dev_id_in_store=dev_id_in_store)
        title = SteamScraperModel.get_steam_id_from_app_id(dev_id).replace("%20", " ")

        bs = WebParseUtils.bs(store_page)
        el = bs.find(attrs={"class": "curator_name"})
        if el is not None:
            title = el.find("a").text

        return {
            "state": 1,
            "title": title,
            "source_data": {
                "store_page": store_page,
            },
            "dev_apps": dev_apps,
            "dev_id_in_store": dev_id_in_store,
            "dev_id": dev_id,
            "url": url
        }

    @staticmethod
    def util_parse_steam_owners(owners: str):
        # "0 .. 20,000"
        owners_arr = owners.split(" .. ")
        owners_from = int(owners_arr[0].replace(",", ""))
        owners_to = int(owners_arr[1].replace(",", ""))
        return owners_from, owners_to

    @staticmethod
    def util_parse_released(page_html_content: str):
        # 21 Aug, 2007
        # Aug 21, 2007
        # Aug 2007

        if release_date_token not in page_html_content:
            return None

        try:
            ix1 = page_html_content.find(release_date_token)
            ix2 = page_html_content.find('</div>', ix1)
            content = page_html_content[ix1 + len(release_date_token): ix2]
            arr = content.split(" ")
            if len(arr) <= 1:
                return None
            for i in range(len(arr)):
                arr[i] = arr[i].strip().removesuffix(",").lower()

            d = 1
            y = int(arr[len(arr) - 1])

            if len(arr) == 2:
                m = months.index(arr[0]) + 1
            else:
                if arr[0] in months:
                    m = months.index(arr[0]) + 1
                    d = int(arr[1])
                else:
                    m = months.index(arr[1]) + 1
                    d = int(arr[0])
            date = datetime(y, m, d).timestamp()
            return date
        except Exception:
            return None

    @staticmethod
    def get_dev_id_in_store_from_store_url(url: str) -> str | None:
        dev_url_base = "https://store.steampowered.com/"

        if dev_url_base not in url:
            return None

        if "/sale/" in url or "curator" in url:
            return None

        dev_types = ["developer", "publisher"]

        found_dev_type = None
        for it in dev_types:
            if f"{dev_url_base}{it}" in url or f"{dev_url_base}search/?{it}" in url:
                found_dev_type = it
                break

        if found_dev_type is None:
            return None

        dev_id_in_store = url.replace(dev_url_base, '')

        ix1 = dev_id_in_store.find("/")
        ix2 = dev_id_in_store.find("/", ix1 + 1)

        if ix2 > ix1:
            dev_id_in_store = dev_id_in_store[:ix2]

        breaks = ['"', "&"]
        if "search/?" not in dev_id_in_store:
            breaks.append("?")

        for it in breaks:
            if it in dev_id_in_store:
                rindex = dev_id_in_store.rindex(it)
                dev_id_in_store = dev_id_in_store[:rindex]

        return dev_id_in_store

    @staticmethod
    def scrap_app(app_id: str):

        steam_app_id = SteamScraperModel.get_steam_id_from_app_id(app_id)

        app_url = f"https://store.steampowered.com/app/{steam_app_id}"
        steam_page = WebParseUtils.get_page_urlopen(app_url)

        bs = WebParseUtils.bs(steam_page)
        icon = None
        el = bs.find(attrs={"class": "apphub_AppIcon"})
        if el is not None:
            icon = el.find("img").get("src")

        found_dev_ids = []
        dev_ids = []
        root_address = "https://store.steampowered.com/"
        root_urls = [
            "###prefix###/",
            "search/?###prefix###="
        ]
        prefixes = [
            "developer",
            "publisher"
        ]
        breaks = [
            "?", '"', "/", "&"
        ]

        for root_url in root_urls:
            for prefix in prefixes:
                i1 = 0
                url = f"{root_address}{root_url.replace('###prefix###', prefix)}"
                while True:
                    i1 = steam_page.find(url, i1)
                    if i1 == -1:
                        break
                    i1 += len(url)
                    i2 = -1
                    for b in breaks:
                        ib = steam_page.find(b, i1)
                        if ib != -1:
                            if i2 == -1 or ib < i2:
                                i2 = ib
                    if i2 != -1:
                        steam_dev_id = steam_page[i1:i2]
                        if steam_dev_id not in found_dev_ids:
                            found_dev_ids.append(steam_dev_id)
                            dev_id = SteamScraperModel.get_app_id_from_steam_id(steam_dev_id)
                            dev_ids.append({
                                "dev_id": dev_id,
                                "dev_id_in_store": f"{root_url.replace('###prefix###', prefix)}{steam_dev_id}"
                            })
            if len(dev_ids) > 0:
                break

        if len(dev_ids) == 0:
            return {
                "state": -4,
                "state_str": f"developer not found for app {app_id}"
            }

        rating = 0
        rating_count = 0

        el = bs.find(attrs={"class": "user_reviews_summary_row", "itemprop": "aggregateRating"})
        if el is not None:
            metas = el.find_all(name="meta", recursive=True)
            for meta in metas:
                itemprop = meta.get("itemprop")
                content = meta.get("content")
                if itemprop == "reviewCount":
                    rating_count = int(content)
                elif itemprop == "ratingValue":
                    rating = int(content) * 10

        owners = int(rating_count / 0.02)

        desc = "N/A"
        el = bs.find("div", {"id": "game_area_description"})
        if el is not None:
            desc = el.text
            desc = desc.replace("About This Game", "")

        store_tags = []
        store_tags_ids = []
        app_tags = bs.find_all(name="a", attrs={"class": "app_tag"})
        if app_tags is not None:
            for it in app_tags:
                store_tag = it.getText().strip()
                store_tag_id = store_tag.lower()
                if store_tag_id not in store_tags_ids:
                    store_tags_ids.append(store_tag_id)
                    store_tags.append(store_tag)

        released = SteamScraperModel.util_parse_released(steam_page)

        reviews = 0

        gallery = []
        ix1 = steam_page.find('id="highlight_strip_scroll"')
        ix2 = steam_page.find('class="slider_ctn"')
        if ix1 != -1 and ix2 != -1:
            while True:
                img_ix1 = steam_page.find('<img src="', ix1)
                if img_ix1 == -1 or img_ix1 > ix2:
                    break
                img_ix1 += len('<img src="')
                img_ix2 = steam_page.find('">', img_ix1)
                img_url = steam_page[img_ix1:img_ix2]
                img_ix3 = img_url.find("?")
                if img_ix3 != -1:
                    img_url = img_url[:img_ix3]
                img_ix3 = img_url.rfind(".")
                if img_ix3 != -1:
                    img_ix4 = img_url.rfind(".", 0, img_ix3 - 1)
                    if img_ix4 != -1:
                        img_url = img_url[:img_ix4] + img_url[img_ix3:]
                gallery.append(img_url)
                ix1 = img_ix2

        el = bs.find(name="div", attrs={"data-price-final": True})
        price = 0
        initial_price = 0
        if el is not None:
            price = int(el.get("data-price-final"))
            initial_price = price
            data_discount = el.get("data-discount")
            if data_discount is not None:
                data_discount = int(data_discount)
                if data_discount > 0:
                    initial_price = int(price / data_discount * 100)

        price = round(price / 100, 2)
        initial_price = round(initial_price / 100, 2)

        el = bs.find(name="div", attrs={"id": "appHubAppName"})
        if el is None:
            return {
                "state": -3,
                "state_str": "title element appHubAppName not found"
            }

        name = el.getText()

        icon_bytes = ScraperUtils.get_app_icon_bytes(icon)

        return {
            "state": 1,
            "reviews": reviews,
            "ratings": rating_count,
            "dev_ids": dev_ids,
            "title": name,
            "icon": icon,
            "description": desc,
            "rank_val": owners,
            "installs": owners,
            "store_tags": store_tags,
            "released": released,
            "rating": rating,
            "source_data": {
                "store_page": steam_page
            },
            "gallery": gallery,
            "price": price,
            "initial_price": initial_price,
            "url": app_url,
            "icon_bytes": icon_bytes,
            "ads": False,
            "iap": False
        }
