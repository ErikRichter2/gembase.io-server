from obfuskey import Obfuskey, alphabets


class UserObfuscator:

    APP_ID_INT = "app_id_int"
    APP_IDS_INT = "app_ids_int"
    TAG_ID_INT = "tag_id_int"
    TAG_IDS_INT = "tag_ids_int"
    DEV_ID_INT = "dev_id_int"
    DEV_IDS_INT = "dev_ids_int"
    AUDIENCE_ANGLE_ID_INT = "audience_angle_id_int"

    __SERVER_TO_CLIENT = {
        TAG_ID_INT: "tag_id",
        TAG_IDS_INT: "tag_ids",
        APP_ID_INT: "app_id",
        APP_IDS_INT: "app_ids",
        DEV_ID_INT: "dev_id",
        DEV_IDS_INT: "dev_ids",
        AUDIENCE_ANGLE_ID_INT: "audience_angle_id"
    }

    __CLIENT_TO_SERVER = {
        "tag_id": TAG_ID_INT,
        "tag_ids": TAG_IDS_INT,
        "app_id": APP_ID_INT,
        "app_ids": APP_IDS_INT,
        "dev_id": DEV_ID_INT,
        "dev_ids": DEV_IDS_INT,
        "audience_angle_id": AUDIENCE_ANGLE_ID_INT
    }

    __known_keys = [
        ["tag_id", "tag_id_int"],
        ["app_id", "app_id_int"],
        ["dev_id", "dev_id_int"]
    ]

    def __init__(self, multiplier: int):
        self.__ignore = False
        self.__obfuscator = Obfuskey(alphabets.BASE36, key_length=8, multiplier=multiplier)

    def set_ignore(self):
        self.__ignore = True

    def server_to_client(self, k: int) -> str | None:
        if k is None:
            return None
        if self.__ignore:
            return str(k)
        return self.__obfuscator.get_key(k)

    def client_to_server(self, v: str) -> int:
        if self.__ignore:
            return int(v)
        return self.__obfuscator.get_value(v)

    def server_to_client_arr(self, k: list):
        for i in range(len(k)):
            k[i] = self.server_to_client(k[i])
        return k

    def client_to_server_arr(self, v: list):
        for i in range(len(v)):
            v[i] = self.client_to_server(v[i])
        return v

    def client_to_server_id(self, o: {}) -> {}:
        for arr in self.__known_keys:
            if arr[0] in o:
                o[arr[1]] = self.client_to_server(o[arr[0]])
                del o[arr[0]]
        return o

    def server_to_client_id(self, o: {}) -> {}:
        for arr in self.__known_keys:
            if arr[1] in o and o[arr[1]] is not None:
                o[arr[0]] = self.server_to_client(o[arr[1]])
                del o[arr[1]]
        return o

    def to_client(self, v: dict | list | None, remove_server_keys=True) -> dict | list | None:
        def __process(d):
            if d is None:
                return

            if isinstance(d, list):
                for it in d:
                    __process(it)
            elif isinstance(d, dict):
                for k in list(d):
                    if k in self.__SERVER_TO_CLIENT:
                        kk = self.__SERVER_TO_CLIENT[k]
                        if isinstance(d[k], list):
                            d[kk] = [self.server_to_client(x) for x in d[k]]
                        else:
                            d[kk] = self.server_to_client(d[k])
                        if remove_server_keys:
                            del d[k]
                    elif isinstance(d[k], (dict, list)):
                        __process(d[k])

        if v is None:
            return None

        __process(v)
        return v

    def to_server(self, v: dict | list | None, remove_client_keys=True) -> dict | list | None:
        def __process(d):
            if d is None:
                return

            if isinstance(d, list):
                for it in d:
                    __process(it)
            elif isinstance(d, dict):
                for k in list(d):
                    if k in self.__CLIENT_TO_SERVER:
                        kk = self.__CLIENT_TO_SERVER[k]
                        if isinstance(d[k], list):
                            d[kk] = [self.client_to_server(x) for x in d[k]]
                        else:
                            if d[k] is None or d[k] == "":
                                d[kk] = d[k]
                            else:
                                d[kk] = self.client_to_server(d[k])
                            if remove_client_keys:
                                del d[k]
                    elif isinstance(d[k], (dict, list)):
                        __process(d[k])

        if v is None:
            return None

        __process(v)
        return v
