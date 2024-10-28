class SurveyUtils:

    @staticmethod
    def is_loved(tag_value: int) -> bool:
        return tag_value == 100

    @staticmethod
    def is_hated(tag_value: int) -> bool:
        return tag_value < 50

    game_name_alias = {
        'lol': 'League of Legends',
        'csgo': 'Counter Strike',
        'cs': 'Counter Strike',
        'cod': 'Call of Duty'
    }

    def normalize_game_name(val: str) -> str:
        tmp: str = val.lower()
        if tmp in SurveyUtils.game_name_alias:
            return SurveyUtils.game_name_alias[tmp]

        tmp_arr: [] = tmp.split(' ')
        for j in range(len(tmp_arr)):
            if tmp_arr[j] == 'of' or tmp_arr[j] == 'the':
                continue
            tmp_arr[j] = tmp_arr[j][:1].upper() + tmp_arr[j][1:]
        tmp = ' '.join(tmp_arr)
        return tmp
