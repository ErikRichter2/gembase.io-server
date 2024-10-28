import json
from urllib.error import URLError
from urllib.parse import quote
from urllib.request import urlopen

from googleapiclient import discovery
from google.oauth2 import service_account

from gembase_server_core.private_data.private_data_model import PrivateDataModel

CUSTOM_SEARCH_URL: str | None = None
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']


def get_google_custom_search_url():
    global CUSTOM_SEARCH_URL
    if CUSTOM_SEARCH_URL is None:
        creds = PrivateDataModel.get_private_data()['google']['custom_search']
        CUSTOM_SEARCH_URL = f"https://www.googleapis.com/customsearch/v1/siterestrict?key={creds['key']}&cx={creds['cx']}&q="
    return CUSTOM_SEARCH_URL


def search(q: str, silent=False) -> {}:
    url = get_google_custom_search_url() + quote(q)
    try:
        fp = urlopen(url)
    except URLError as err:
        if silent:
            return None
        else:
            raise err
    content = fp.read().decode("utf8")
    return json.loads(content)


def get_credentials():
    credentials_json = PrivateDataModel.get_private_data()['google']['service_account']
    credentials = service_account.Credentials.from_service_account_info(credentials_json, scopes=SCOPES)
    return credentials


def read_sheet(sheet_id: str, range_id: str, to_arr_dict: bool = False):
    service = discovery.build('sheets', 'v4', credentials=get_credentials())
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=sheet_id, range=range_id).execute()
    values = result.get('values', [])
    if to_arr_dict:
        values_dict = []
        for i in range(1, len(values)):
            it = {}
            for j in range(len(values[0])):
                col = values[0][j]
                if "[IGNORE]" in col:
                    continue
                if len(values[i]) > j:
                    it[col] = values[i][j]
                else:
                    it[col] = ''
            values_dict.append(it)
        return values_dict
    else:
        return values


def sheet_to_dict(sheet_id: str, ranges: [] = None):
    d = {}

    if ranges is None:
        ranges = get_sheets(sheet_id)

    for r in ranges:
        d[r] = read_sheet(sheet_id, r, True)

    return d


def write_sheet(sheet_id: str, range_id: str, values: []):
    service = discovery.build('sheets', 'v4', credentials=get_credentials())
    sheet = service.spreadsheets()
    body = {
        'values': values
    }
    request = sheet.values().update(spreadsheetId=sheet_id, range=range_id, valueInputOption='USER_ENTERED', body=body)
    res = request.execute()
    return res


def get_sheets(sheet_id: str) -> []:
    sheets = []
    service = discovery.build('sheets', 'v4', credentials=get_credentials())
    sheet_metadata = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
    for it in sheet_metadata["sheets"]:
        sheets.append(it["properties"]["title"])
    return sheets
