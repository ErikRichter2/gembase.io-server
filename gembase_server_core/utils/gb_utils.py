import datetime
import json
import os
import string
import sys
import uuid
import zlib
from pathlib import Path
import random
from urllib.request import urlopen

import pyarrow as pa
import pyarrow.feather as feather
from flask import request

from gembase_server_core.environment.runtime_constants import rr


class GbUtils:

    @staticmethod
    def timestamp() -> float:
        return datetime.datetime.now().timestamp()

    @staticmethod
    def timestamp_int() -> int:
        return int(GbUtils.timestamp())

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
        if not GbUtils.has_arg(arg):
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
        if val is None:
            return ""

        is_negative = False
        if val < 0:
            is_negative = True
            val = abs(val)

        if val == 0:
            return "0"

        def add_sign(v: str):
            if is_negative:
                return f"-{v}"
            return v

        M = 1000 * 1000
        B = 1000 * 1000 * 1000

        if val < 1000:
            return add_sign(str(val))
        elif val < M:
            val = round(val / 1000)
            if val > 100:
                val = round(val / 10) * 10
            return add_sign(f"{str(val)}k")
        elif max_m or val < B:
            val = round(val / M)
            if val > 100:
                val = round(val / 10) * 10
            return add_sign(f"{str(val)}M")
        else:
            val_b = round(val / B)
            if val_b > 100:
                val_b = round(val_b / 10) * 10
            return add_sign(f"{str(val_b)}B")

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
    def is_string(val) -> bool:
        if val is None:
            return False
        return True

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
        return True

    @staticmethod
    def is_bool(val):
        if val is None:
            return False
        return isinstance(val, bool)

    @staticmethod
    def try_dict_key(o: dict, key: str, default: any = None) -> any:
        return default if key not in o else o[key]

    @staticmethod
    def get_token_from_request_header() -> str | None:
        token = None
        if 'Authorization' in request.headers:
            if request.headers['Authorization'] is not None:
                token = request.headers['Authorization'][6:]
                if token == 'null':
                    token = None
        return token

    @staticmethod
    def get_project_root_path(project_root_file="project_root") -> Path:
        p: Path = Path(__file__)
        while True:
            if os.path.exists(os.path.join(p, project_root_file)):
                break
            p = p.parent
            if p is None:
                raise Exception("Project root path not found")

        return p
