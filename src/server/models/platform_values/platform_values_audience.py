import math

from src.server.models.user.user_obfuscator import UserObfuscator


class PlatformValuesAudienceStats:
    def __init__(
            self,
            loved_survey_cnt: int,
            total_survey_cnt: int,
            rejected_survey_cnt: int,
            potential_downloads: int,
            loyalty_installs: int,
            app_platforms: [int],
            loved_ratio_ext: float,
            rejected_ratio_ext: float,
            installs: int,
            is_admin=False
    ):
        self.survey_loved_cnt = loved_survey_cnt
        if loved_ratio_ext > 0:
            self.survey_loved_cnt = int(loved_ratio_ext * total_survey_cnt)

        rejected_survey_cnt_hack = rejected_survey_cnt
        if rejected_ratio_ext > 0:
            rejected_survey_cnt_hack = int(rejected_ratio_ext * rejected_survey_cnt)

        # RR = RSR * ( 1 + SIN ( RSR / 2 ) * 0.25 )
        rejected_survey_cnt_hack = int(rejected_survey_cnt_hack * (1 + math.sin(rejected_survey_cnt_hack / 2) * 0.25))

        self.survey_rejected_cnt = rejected_survey_cnt_hack

        self.survey_total_cnt = total_survey_cnt

        self.app_platforms = {
            UserObfuscator.TAG_IDS_INT: app_platforms
        }

        self.max_audience = potential_downloads

        self.loved_ratio = 0
        if total_survey_cnt > 0:
            self.loved_ratio = self.survey_loved_cnt / total_survey_cnt

        self.loved_absolute = self.loved_ratio * self.max_audience

        self.rejected_ratio = 0
        if self.survey_loved_cnt > 0:
            self.rejected_ratio = self.survey_rejected_cnt / self.survey_loved_cnt

        self.rejected_absolute = self.loved_absolute * self.rejected_ratio

        self.loyal_ratio = 0
        self.loyal_absolute = loyalty_installs
        if self.loved_absolute > 0:
            self.loyal_ratio = loyalty_installs / self.loved_absolute
            self.loyal_absolute = loyalty_installs

        self.total_audience = self.loved_absolute - (self.rejected_absolute + self.loyal_absolute)

        self.no_data = False
        if self.total_audience < 0:
            self.total_audience = 0
            self.no_data = True

        self.total_audience_ratio = round(self.total_audience / self.max_audience, 2)

        self.admin_data = None
        if is_admin:
            self.admin_data = {
                "loved_survey_cnt": loved_survey_cnt,
                "total_survey_cnt": total_survey_cnt,
                "rejected_survey_cnt": rejected_survey_cnt,
                "potential_downloads": potential_downloads,
                "loyalty_installs": loyalty_installs,
                "loved_ratio_ext": loved_ratio_ext,
                "rejected_ratio_ext": rejected_ratio_ext,
                "rejected_ratio_final": self.rejected_ratio,
                "installs": installs,
                "survey_rejected_cnt_hack": rejected_survey_cnt_hack
            }

    def generate_client_data(self, locked=False):
        if locked:
            return {
                "locked": True,
            }
        else:
            return {
                "survey_loved_cnt": self.survey_loved_cnt,
                "survey_total_cnt": self.survey_total_cnt,
                "survey_rejected_cnt": self.survey_rejected_cnt,
                "max_audience": self.max_audience,
                "loved_ratio": round(self.loved_ratio, 2),
                "loved_absolute": self.loved_absolute,
                "rejected_ratio": round(self.rejected_ratio, 2),
                "rejected_absolute": self.rejected_absolute,
                "loyal_ratio": round(self.loyal_ratio, 2),
                "loyal_absolute": self.loyal_absolute,
                "total_audience": self.total_audience,
                "total_audience_ratio": self.total_audience_ratio,
                "no_data": self.no_data,
                "app_platforms": self.app_platforms,
                "admin_data": self.admin_data
            }
