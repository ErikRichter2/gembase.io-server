import datetime
from io import BytesIO
import json
import os
import string
import sys
from typing import Optional, TypeVar
from urllib.request import urlopen
import uuid
import zlib
from pathlib import Path
import random
import base64
import hashlib

import pyarrow as pa

from PIL import Image
from dateutil import parser, tz
from flask import request
from pyarrow import feather

from server_core.db.db_connection import DbConnection
from src.env_data import APP_ENV_PROD, ENV_DATA


ISO_DATE_TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

class Utils:
    
    @staticmethod
    def dict_to_list(d: dict[int, dict]) -> list[dict]:
        arr = []
        for k in d:
            arr.append(d[k])
        return arr
        
    
    @staticmethod
    def int_safe(val: str) -> int:
        res = 0
        try:
            res = int(val)
        except Exception:
            pass
        return res

    @staticmethod
    def timestamp() -> float:
        return Utils.utc_now().timestamp()

    @staticmethod
    def timestamp_int() -> int:
        return int(Utils.timestamp())

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
        if ENV_DATA.app_env != APP_ENV_PROD:
            print(log)

    @staticmethod
    def has_arg(arg: str) -> bool:
        if arg in sys.argv:
            return True
        return False

    @staticmethod
    def get_arg(arg: str, default=None):
        if not Utils.has_arg(arg):
            return default
        i = sys.argv.index(arg)
        return sys.argv[i + 1]

    @staticmethod
    def compress(data: str):
        b = bytes(data, "utf-8")
        c = zlib.compress(b)
        return c

    @staticmethod
    def decompress(data: bytes):
        b = zlib.decompress(data)
        s = b.decode("utf-8")
        return s

    @staticmethod
    def get_guid() -> str:
        return str(uuid.uuid4())

    T = TypeVar('T', bound=object)
    @staticmethod
    def json_copy(o: T) -> T:
        return json.loads(json.dumps(o))

    @staticmethod
    def hash(text: str):
        res = 0
        for ch in text:
            res = (res * 281 ^ ord(ch) * 997) & 0xFFFFFFFF
        return res

    @staticmethod
    def random_from_array(arr: list):
        if len(arr) == 0:
            return None
        rnd_index = random.randrange(0, len(arr))
        return arr[rnd_index]

    @staticmethod
    def is_guid(val) -> bool:
        if val is None:
            return False

        try:
            uuid_obj = uuid.UUID(val, version=4)
        except ValueError:
            return False

        val_str = str(uuid_obj)

        return val_str == val

    @staticmethod
    def is_string(val, max_length: int=100) -> bool:
        if val is None:
            return False
        return True

    @staticmethod
    def is_bool(val, optional=False) -> bool:
        if val is None:
            if optional:
                return True
            return False
        return True

    @staticmethod
    def is_int(val) -> bool:
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

    @staticmethod
    def sha256(value, secret=None):
        m = hashlib.new('sha256')
        value_bytes = bytes(value, 'utf-8')
        m.update(value_bytes)
        if secret is None:
            secret = base64.b64encode(os.urandom(32))
        m.update(secret)
        value_hash = m.hexdigest()
        return value_hash, secret

    @staticmethod
    def compare_hash(value, secret, compare_to_hash):
        my_hash, secret = Utils.sha256(value, secret)
        return my_hash == compare_to_hash
    
    @staticmethod
    def get_api_header(api_key: Optional[str]):
        return {"X-API-KEY": api_key} if api_key is not None else None

    @staticmethod
    def is_valid_iso8601(ts: str) -> bool:
        try:
            datetime.datetime.strptime(ts, ISO_DATE_TIME_FORMAT)
            return True
        except ValueError:
            return False
        
    @staticmethod
    def utc_now():
        return datetime.datetime.now(tz=tz.UTC)

    @staticmethod
    def datetime_to_utc_db(d: datetime.datetime):
        return d.astimezone(tz.UTC).strftime('%y-%m-%d %H:%M:%S')
    
    @staticmethod
    def datetime_from_utc_iso8601(s: str):
        return parser.parse(s).astimezone(tz.UTC)

    @staticmethod
    def datetime_to_utc_iso8601(d: datetime.datetime):
        return d.astimezone(tz.UTC).strftime(ISO_DATE_TIME_FORMAT)

    @staticmethod
    def utc_datetime(year: int, month: int, day: int, hour: int = 0, minute: int = 0, second: int = 0):
        return datetime.datetime(year, month, day, hour, minute, second, tzinfo=datetime.timezone.utc)
    
    @staticmethod
    def get_symbol_map(conn: DbConnection, exchange: str):
        rows = conn.select_all("""
                            SELECT m.id, 
                                    m.name_native as name_db,
                                    IFNULL(o.name, m.name_native) as name_exchange,
                                    IFNULL(o.price_multiply, 1) as price_multiply
                                FROM algo.map_currency m
                                LEFT JOIN algo.exchange_override_currency_name o
                                ON o.currency_id = m.id
                                AND o.exchange_type = %s
                                WHERE m.agent_symbol = 1
                            """, [exchange])
        
        m: dict = {}
        m['__ids__'] = []
        for row in rows:
            row_id = int(row['id'])
            m['__ids__'].append(row_id)
            m[str(row_id)] = {
                'name_db': row['name_db'],
                'name_exchange': row['name_exchange'],
                'price_multiply': row['price_multiply']
            }
            m[row['name_db']] = row['id']
            m[row['name_exchange']] = row['id']
            
        return m    
    
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
    def is_string_enum(val, enum_vals: list[str]) -> bool:
        if Utils.is_string(val):
            if val in enum_vals:
                return True
        return False    
    
    @staticmethod
    def load_page(url: str) -> str | None:
        try:
            fp = urlopen(url)
            return fp.read().decode("utf8")
        except Exception:
            return None
        
    @staticmethod
    def img_to_thumbnail_bytes(img: Image.Image, size: tuple[float, float], file_format="PNG") -> bytes:
        img.thumbnail(size, Image.LANCZOS)
        output = BytesIO()
        img.save(output, format=file_format)
        data = output.getvalue()
        return data
    
    @staticmethod
    def set_if_not_none(d: dict, key: str, val: object) -> bool:
        if val is not None:
            d[key] = val
            return True
        return False    

    @staticmethod
    def try_get_from_dict(d: dict, *keys) -> object:
        for k in keys:
            if k in d and d[k] is not None:
                d = d[k]
            else:
                return None
        return d
    
    @staticmethod
    def round_price(x: float) -> float:
        if x > 1000:
            return int(x)
        c = int(x) + 1
        if x < c:
            x = c - 0.01
        return x
    
    @staticmethod
    def format_price(x: float) -> str:
        return f"{x:,}"
        
    @staticmethod
    def format_number(val: int | float | None, max_m=True) -> str:
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

        if val < 1000:
            return add_sign(str(val))
        elif val < 1000000:
            val = round(val / 1000)
            if val > 100:
                val = round(val / 10) * 10
            return add_sign(f"{str(val)}k")
        elif max_m or val < 1000000000:
            val = round(val / 1000000)
            if val > 1000:
                val = round(val / 100) * 100
            elif val > 100:
                val = round(val / 10) * 10
            return add_sign(f"{str(val)}M")
        else:
            val_b = round(val / 1000000000)
            if val_b > 10:
                val_b = round(val_b / 10) * 10
            else:
                val_b = round(val / 100000000) / 10
            return add_sign(f"{str(val_b)}B")        
        
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
        