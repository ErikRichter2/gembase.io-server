import json
import os
import random
import string
import sys
import uuid
import zlib
from datetime import datetime
from io import BytesIO
from urllib.request import urlopen

import pyarrow as pa
import pyarrow.feather as feather
from PIL import Image

from gembase_server_core.db.db_connection import DbConnection
from gembase_server_core.environment.runtime_constants import rr
from gembase_server_core.private_data.private_data_model import PrivateDataModel
from gembase_server_core.utils.gb_utils import GbUtils


class GembaseUtils:

    @staticmethod
    def client_url_root():
        return PrivateDataModel.get_private_data()['gembase']['client']['url_root']

    @staticmethod
    def timestamp() -> float:
        return datetime.now().timestamp()

    @staticmethod
    def timestamp_int() -> int:
        return int(GembaseUtils.timestamp())

    @staticmethod
    def name_to_col(name: str):
        return name.replace("-", "_").replace(" ", "_").replace("%", "perc").replace("+", "plus").lower()

    @staticmethod
    def get_email_domain(email: str) -> str:
        at = '@'
        if at not in email:
            raise Exception("Invalid email")
        at_i = email.index(at)
        domain = email[(at_i + 1):]
        return domain

    @staticmethod
    def debug_log(log: str):
        if not rr.is_prod():
            print(log)

    @staticmethod
    def db_data_to_df(data):
        reader = pa.BufferReader(data)
        df = feather.read_feather(reader)
        return df

    @staticmethod
    def db_data_to_json(data):
        if isinstance(data, str):
            return json.loads(data)
        else:
            data_str = data.decode('utf-8')
            return json.loads(data_str)

    @staticmethod
    def has_arg(arg: str) -> bool:
        if arg in sys.argv:
            return True
        return False

    @staticmethod
    def get_arg(arg: str, default=None):
        if not GembaseUtils.has_arg(arg):
            return default
        i = sys.argv.index(arg)
        return sys.argv[i + 1]

    @staticmethod
    def float_safe(val: str) -> float:
        res = 0
        try:
            res = float(val)
        except Exception:
            pass
        return res

    @staticmethod
    def int_safe(val: str) -> int:
        res = 0
        try:
            res = int(val)
        except Exception:
            pass
        return res

    @staticmethod
    def compress(data: str):
        b = bytes(data, 'utf-8')
        c = zlib.compress(b)
        return c

    @staticmethod
    def decompress(data: bytes):
        b = zlib.decompress(data)
        s = b.decode("utf-8")
        return s

    @staticmethod
    def log_service(data: str):
        pid = os.getpid()
        conn = DbConnection()
        conn.query("""
        UPDATE app_temp_data.server_services s
           SET s.log = %s
         WHERE s.pid = %s
        """, [data, pid])
        conn.commit()
        conn.close()
        print(data)

    @staticmethod
    def load_page(url: str) -> str | None:
        try:
            fp = urlopen(url)
            return fp.read().decode("utf8")
        except Exception:
            return None

    @staticmethod
    def get_guid() -> str:
        return str(uuid.uuid4())

    @staticmethod
    def format_number(val: int | None, max_m=True) -> str:
        return GbUtils.format_number(val=val, max_m=max_m)

    @staticmethod
    def compare_arr(arr1: [], arr2: []):
        if len(arr1) != len(arr2):
            return False
        for it1 in arr1:
            if it1 not in arr2:
                return False
        for it2 in arr2:
            if it2 not in arr1:
                return False
        return True

    @staticmethod
    def compare_str(str1: str, str2: str) -> bool:
        if str1 is None and str2 is not None:
            return False
        if str2 is None and str1 is not None:
            return False
        if str1 is None and str2 is None:
            return True

        str1 = str1.lower().translate({ord(c): None for c in string.whitespace})
        str2 = str2.lower().translate({ord(c): None for c in string.whitespace})

        return str1 == str2

    @staticmethod
    def json_copy(o: any) -> any:
        return json.loads(json.dumps(o))

    @staticmethod
    def hash(text: str):
        res = 0
        for ch in text:
            res = (res * 281 ^ ord(ch) * 997) & 0xFFFFFFFF
        return res

    @staticmethod
    def random_from_array(arr: []):
        if len(arr) == 0:
            return None
        rnd_index = random.randrange(0, len(arr))
        return arr[rnd_index]

    @staticmethod
    def format_price(x: float) -> str:
        return f"{x:,}"

    @staticmethod
    def round_price(x: float) -> float:
        if x > 1000:
            return int(x)
        c = int(x) + 1
        if x < c:
            x = c - 0.01
        return x

    @staticmethod
    def is_guid(val) -> bool:
        if val is None:
            return False

        return True

        try:
            uuid_obj = uuid.UUID(val, version=4)
        except ValueError:
            return False

        val_str = str(uuid_obj)

        return val_str == val

    @staticmethod
    def is_defined(d: dict, *keys) -> bool:
        for k in keys:
            if k in d and d[k] is not None:
                d = d[k]
            else:
                return False
        return True

    @staticmethod
    def try_get_from_dict(d: dict, *keys) -> any:
        for k in keys:
            if k in d and d[k] is not None:
                d = d[k]
            else:
                return None
        return d

    @staticmethod
    def set_if_not_none(d: dict, key: str, val: any) -> bool:
        if val is not None:
            d[key] = val
            return True
        return False

    @staticmethod
    def is_string(val, max_length: int = -1) -> bool:
        if val is not None and isinstance(val, str):
            return True if max_length == -1 or len(val) <= max_length else False
        return False

    @staticmethod
    def is_string_enum(val, enum_vals: list[str]) -> bool:
        if GembaseUtils.is_string(val):
            if val in enum_vals:
                return True
        return False

    @staticmethod
    def is_email(val) -> bool:
        if val is None:
            return False
        try:
            if "@" not in val or "." not in val:
                return False
        except Exception:
            return False
        return True

    @staticmethod
    def is_int(val):
        if val is None:
            return False
        try:
            int(val)
            return True
        except Exception:
            return False

    @staticmethod
    def is_bool(val):
        if val is None:
            return False
        return isinstance(val, bool)

    @staticmethod
    def img_to_thumbnail_bytes(img: Image, size: [int, int], file_format="PNG") -> bytes:
        img.thumbnail(size, Image.LANCZOS)
        output = BytesIO()
        img.save(output, format=file_format)
        data = output.getvalue()
        return data
