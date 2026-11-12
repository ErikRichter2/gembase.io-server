from src.env_data import ENV_DATA


class SurveyUtils:
    
    game_name_alias = {
        'lol': 'League of Legends',
        'csgo': 'Counter Strike',
        'cs': 'Counter Strike',
        'cod': 'Call of Duty'
    }
        
    @staticmethod
    def get_export_for_end_page(end_type: str, config: dict):
        title = None
        redirect = None
        subtitle = None

        for it in config["end"]:
            if it["param"] == "title" and it["value"] == end_type:
                title = it["text"]
            if it["param"] == "redirect" and it["value"] == end_type:
                redirect = it["value_2"]
            if it["param"] == "subtitle":
                subtitle = it["text"]

        if not ENV_DATA.is_prod():
            redirect = "https://www.google.com"

        return {
            "config_data": {
                "id": "end",
                "template": "end",
                "title": title,
                "subtitle": subtitle,
                "redirect": redirect,
                "end_type": end_type
            }
        }
    


    @staticmethod
    def is_loved(tag_value: int) -> bool:
        return tag_value == 100

    @staticmethod
    def is_hated(tag_value: int) -> bool:
        return tag_value < 50

    @staticmethod
    def normalize_game_name(val: str) -> str:
        tmp: str = val.lower()
        if tmp in SurveyUtils.game_name_alias:
            return SurveyUtils.game_name_alias[tmp]

        tmp_arr: list[str] = tmp.split(' ')
        for j in range(len(tmp_arr)):
            if tmp_arr[j] == 'of' or tmp_arr[j] == 'the':
                continue
            tmp_arr[j] = tmp_arr[j][:1].upper() + tmp_arr[j][1:]
        tmp = ' '.join(tmp_arr)
        return tmp    