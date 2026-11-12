class AudienceStats:
    def __init__(
            self,
            loved_survey_cnt: int,
            total_survey_cnt: int,
            rejected_survey_cnt: int,
            potential_downloads: int,
            loyalty_installs: int,
            no_data: bool = False
    ):
        self.survey_loved_cnt = loved_survey_cnt
        self.survey_total_cnt = total_survey_cnt
        self.survey_rejected_cnt = rejected_survey_cnt

        self.no_data = no_data
        self.max_audience = potential_downloads

        self.loved_ratio = 0
        if total_survey_cnt > 0:
            self.loved_ratio = loved_survey_cnt / total_survey_cnt

        self.loved_absolute = self.loved_ratio * self.max_audience

        self.rejected_ratio = 0
        if loved_survey_cnt > 0:
            self.rejected_ratio = rejected_survey_cnt / loved_survey_cnt

        self.rejected_absolute = self.loved_absolute * self.rejected_ratio

        self.loyal_ratio = 0
        self.loyal_absolute = loyalty_installs
        if self.loved_absolute > 0:
            self.loyal_ratio = loyalty_installs / self.loved_absolute
            self.loyal_absolute = loyalty_installs

        self.total_audience = self.loved_absolute - (self.rejected_absolute + self.loyal_absolute)

        if self.total_audience < 0:
            self.total_audience = 0
            self.no_data = True

        self.total_audience_ratio = round(self.total_audience / self.max_audience, 2)

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
                "no_data": self.no_data
            }
