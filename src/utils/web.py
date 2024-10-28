import sys
from urllib.request import urlopen

import requests
from bs4 import BeautifulSoup
from pyvirtualdisplay import Display
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


class WebParseUtils:

    @staticmethod
    def get_page_head_request(url: str) -> any:
        header = None

        try:
            header = requests.head(url)
        except Exception:
            pass

        return header

    @staticmethod
    def get_page_urlopen_safe(url: str) -> str | None:
        page_content = None

        try:
            fp = urlopen(url)
            page_content = fp.read().decode("utf8")
        except Exception:
            pass

        return page_content

    @staticmethod
    def get_page_urlopen(url: str) -> str:
        fp = urlopen(url)
        return fp.read().decode("utf8")

    @staticmethod
    def bs(content: str) -> BeautifulSoup:
        return BeautifulSoup(content, 'html.parser')


def parse_web_page(url: str) -> BeautifulSoup:
    return BeautifulSoup(get_web_page(url), 'html.parser')


def get_web_page(url: str) -> str:
    content = get_driver(url).page_source
    return content


def get_driver(url: str):
    if sys.platform == "linux":
        display = Display(visible=False, size=(800, 600))
        display.start()

    options = Options()
    options.add_argument("--no-sandbox")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager(version='114.0.5735.90').install()), options=options)
    driver.get(url)
    return driver
