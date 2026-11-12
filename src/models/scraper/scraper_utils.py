from io import BytesIO

import requests
from PIL import Image
from werkzeug.datastructures import FileStorage

from server_core.utils.utils import Utils

class ScraperUtils:

    __APP_ICON_SIZE = 128, 128

    @staticmethod
    def get_app_icon_bytes_from_file(file: FileStorage) -> bytes | None:
        try:
            return Utils.img_to_thumbnail_bytes(
                img=Image.open(file.stream),
                size=ScraperUtils.__APP_ICON_SIZE
            )
        except Exception:
            return None

    @staticmethod
    def get_app_icon_bytes(icon_url: str) -> bytes | None:
        try:
            response = requests.get(icon_url)
            return Utils.img_to_thumbnail_bytes(
                img=Image.open(BytesIO(response.content)),
                size=ScraperUtils.__APP_ICON_SIZE
            )
        except Exception:
            return None
