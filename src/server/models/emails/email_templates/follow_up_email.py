from gembase_server_core.private_data.private_data_model import PrivateDataModel
from src import external_api
from src.server.models.apps.app_model import AppModel
from src.server.models.tags.tags_mapper import TagsMapper
from src.server.models.user.user_obfuscator import UserObfuscator
from src.server.models.platform_values.calc.platform_values_audience_angle_calc import \
    PlatformValuesAudienceAngleCalc
from src.server.models.platform_values.calc.platform_values_competitors_calc import PlatformValuesCompetitorsCalc
from src.server.models.emails.email_templates.base_email import BaseEmail
from src.server.models.user.user_registration_helper import UserRegistrationHelper
from src.server.models.scraper.scraper_model import ScraperModel
from src.server.models.tags.tags_constants import TagsConstants

FOLLOW_UP_EMAIL_ID = "follow_up"


class FollowUpEmail(BaseEmail):

    def __get_user_data_from_email(self):
        row = self.conn.select_one_or_none("""
        SELECT ud.dev_id_int, u.name
          FROM app.users u, app.users_devs ud
          WHERE u.email = %s and u.id = ud.user_id
        """, [self.email])

        if row is None:
            row = self.conn.select_one_or_none("""
            SELECT oo.dev_id_int, o.name
              FROM app.organization_requests o, app.organization oo
              WHERE o.email = %s
              and o.organization_id = oo.id
            """, [self.email])

        if row is None:
            row = self.conn.select_one_or_none("""
                SELECT r.dev_id_int, r.name
                  FROM app.users_registration_requests r
                  WHERE r.email = %s
                """, [self.email])

        if row is None:
            return {
                "dev_id_int": 0,
                "name": ""
            }

        return row

    def set_parameters(
            self,
            app_id_int: int = None,
            audience_angle_id_int: int | None = None
    ):
        survey_id = 3
        platform_id = 1
        competitors_platform_id = 2
        obfuscator = UserObfuscator(1)
        obfuscator.set_ignore()
        map_tags = TagsMapper(conn=self.conn).map_tags

        most_downloaded_game_title = "<b>!!! N/A !!!</b>"
        largest_audience = "<b>!!! N/A !!!</b>"
        top_3_behaviors = ["<b>!!! N/A !!!</b>" for i in range(3)]
        top_3_needs = ["<b>!!! N/A !!!</b>" for i in range(3)]
        top_domain = "<b>!!! N/A !!!</b>"
        top_genre = "<b>!!! N/A !!!</b>"
        top_topic = "<b>!!! N/A !!!</b>"
        top_2_competitors = ["<b>!!! N/A !!!</b>" for i in range(2)]
        all_angles = []
        all_apps = []

        self.conn.query("""
                DELETE FROM platform_values.results_audience_angles__competitors
                WHERE platform_id IN (%s, %s)
                """, [platform_id, competitors_platform_id])

        self.conn.query("""
                        DELETE FROM platform_values.results_audience_angles__final
                        WHERE platform_id IN (%s, %s)
                        """, [platform_id, competitors_platform_id])

        self.conn.commit()

        def get_name_for_audience(angle_data_tags):

            if len(angle_data_tags) == 0:
                return ""

            if len(angle_data_tags) == 1:
                return map_tags["i2n"][angle_data_tags[0]]
            else:
                def1 = map_tags["i2def"][angle_data_tags[0]]
                def2 = map_tags["i2def"][angle_data_tags[1]]

                if def1["subcategory_int"] == TagsConstants.SUBCATEGORY_TOPICS_ID:
                    if def1["adj"] is not None and def1["adj"] != "":
                        return f"{def1['adj']} {def2['node']}"
                    else:
                        return f"{def1['node']} {def2['node']}"

                if def1["subcategory_int"] == TagsConstants.SUBCATEGORY_TOPICS_ID:
                    if def2["adj"] is not None and def2["adj"] != "":
                        return f"{def2['adj']} {def1['node']}"
                    else:
                        return f"{def2['node']} {def1['node']}"

                if def1["adj"] is not None and def1["adj"] != "":
                    return f"{def1['adj']} {def2['node']}"
                elif def2["adj"] is not None and def2["adj"] != "":
                    return f"{def2['adj']} {def1['node']}"
                else:
                    return f"{def1['node']} {def2['node']}"

        user_data = self.__get_user_data_from_email()

        if user_data is not None:
            dev_id_int = user_data["dev_id_int"]

            if dev_id_int != 0:

                dev_detail = AppModel.get_devs_details(
                    conn=self.conn,
                    devs_ids_int=[dev_id_int]
                )[dev_id_int]

                if dev_detail["store"] != AppModel.STORE__CONCEPT:

                    if not ScraperModel.is_dev_scraped(
                            conn=self.conn,
                            dev_id_int=dev_id_int,
                            days_since_last=7
                    ):
                        ScraperModel.scrap_dev(
                            conn=self.conn,
                            dev_id_in_store=dev_detail["dev_id_in_store"],
                            store=dev_detail["store"],
                            scrap_dev_apps=True
                        )

                    rows_apps = self.conn.select_all("""
                            SELECT a.app_id_int, a.title, a.store
                              FROM scraped_data.apps_valid a,
                                   scraped_data.devs_apps da
                             WHERE da.app_id_int = a.app_id_int
                               AND da.dev_id_int = %s
                               AND a.released IS NOT NULL
                               ORDER BY a.installs DESC
                            """, [dev_id_int])

                    if len(rows_apps) > 0:

                        all_apps = [
                            {
                                UserObfuscator.APP_ID_INT: row[UserObfuscator.APP_ID_INT],
                                "title": row["title"]
                            } for row in rows_apps
                        ]

                        app_id_int = rows_apps[0][UserObfuscator.APP_ID_INT]

                        if app_id_int is not None:
                            for row in rows_apps:
                                if row[UserObfuscator.APP_ID_INT] == app_id_int:
                                    most_downloaded_game_title = row["title"]
                                    break
                        else:
                            most_downloaded_game_title = rows_apps[0]["title"]

                        app_tier = AppModel.get_tier_2(conn=self.conn, app_id_int=app_id_int)

                        my_growth = 0
                        row_growth = self.conn.select_one_or_none("""
                        select growth from platform.platform_values_apps where app_id_int = %s
                        """, [app_id_int])
                        if row_growth is not None:
                            my_growth = row_growth["growth"]

                        tags = AppModel.get_tags(
                            conn=self.conn,
                            user_id=1,
                            app_id_int=app_id_int
                        )["tags"]

                        ranked_tags = [tag_detail for tag_detail in tags if tag_detail["tag_rank"] != 0]

                        if len(ranked_tags) > 0:

                            # audience angles
                            PlatformValuesAudienceAngleCalc.calc(
                                conn=self.conn,
                                survey_id=survey_id,
                                platform_id=platform_id,
                                dev_id_int=dev_id_int,
                                tag_details=ranked_tags,
                            )

                            data = PlatformValuesAudienceAngleCalc.generate_client_data(
                                conn=self.conn,
                                platform_id=platform_id
                            )

                            data.sort(key=lambda x: x["audience_stats"]["total_audience"], reverse=True)

                            for it in data:
                                all_angles.append({
                                    UserObfuscator.AUDIENCE_ANGLE_ID_INT: it[UserObfuscator.AUDIENCE_ANGLE_ID_INT],
                                    "name": get_name_for_audience([tag_id_int for tag_id_int in it[UserObfuscator.TAG_IDS_INT]])
                                })

                            if len(data) > 0:
                                top_angle = data[0]
                                if audience_angle_id_int is not None:
                                    for it in data:
                                        if it[UserObfuscator.AUDIENCE_ANGLE_ID_INT] == audience_angle_id_int:
                                            top_angle = it
                                            break
                                else:
                                    audience_angle_id_int = top_angle[UserObfuscator.AUDIENCE_ANGLE_ID_INT]
                                angle_tags = top_angle[UserObfuscator.TAG_IDS_INT]
                                for i in range(len(angle_tags)):
                                    angle_tags[i] = int(angle_tags[i])

                                largest_audience = get_name_for_audience(angle_tags)

                                arr = []
                                for tag_id_int in top_angle["top_behaviors"][UserObfuscator.TAG_IDS_INT]:
                                    arr.append(map_tags["i2n"][tag_id_int])
                                top_3_behaviors = arr
                                arr = []
                                for tag_id_int in top_angle["top_needs"][UserObfuscator.TAG_IDS_INT]:
                                    arr.append(map_tags["i2n"][tag_id_int])
                                top_3_needs = arr
                                if len(top_angle["top_domains"][UserObfuscator.TAG_IDS_INT]) > 0:
                                    top_domain = map_tags["i2n"][int(top_angle["top_domains"][UserObfuscator.TAG_IDS_INT][0])]

                                for tag_id_int in top_angle["top_genres"][UserObfuscator.TAG_IDS_INT]:
                                    if tag_id_int not in angle_tags:
                                        top_genre = map_tags["i2n"][tag_id_int]
                                        break

                                for tag_id_int in top_angle["top_topics"][UserObfuscator.TAG_IDS_INT]:
                                    if tag_id_int not in angle_tags:
                                        top_topic = map_tags["i2n"][tag_id_int]
                                        break

                                PlatformValuesCompetitorsCalc.find_competitors_for_audience_angle(
                                    conn=self.conn,
                                    platform_id=competitors_platform_id,
                                    survey_id=survey_id,
                                    my_tier=app_tier,
                                    my_growth=my_growth,
                                    my_tags_details=tags,
                                    exclude_apps_from_competitors=[app_id_int],
                                    audience_angle_row_id=top_angle["row_id"],
                                    dev_id_int=dev_id_int
                                )

                                data = PlatformValuesCompetitorsCalc.generate_client_data(
                                    conn=self.conn,
                                    platform_id=competitors_platform_id
                                )

                                if len(data["ts_items"]) >= 2:
                                    top_2_competitors = [
                                        AppModel.get_app_detail(
                                            conn=self.conn,
                                            app_id_int=int(data["ts_items"][0][UserObfuscator.APP_ID_INT]),
                                            user_id=1
                                        )["title"],
                                        AppModel.get_app_detail(
                                            conn=self.conn,
                                            app_id_int=int(data["ts_items"][1][UserObfuscator.APP_ID_INT]),
                                            user_id=1
                                        )["title"]
                                    ]

        self.instance_data.content_parameters["[MOST_DOWNLOADED_GAME]"] = most_downloaded_game_title
        self.instance_data.content_parameters["[LARGEST_AUDIENCE]"] = largest_audience
        self.instance_data.content_parameters["[TOP_NON_AUDIENCE_GENRE]"] = top_genre
        self.instance_data.content_parameters["[TOP_NON_AUDIENCE_TOPIC]"] = top_topic
        self.instance_data.content_parameters["[TOP_DOMAIN]"] = top_domain
        self.instance_data.content_parameters["[TOP_1_BEHAVIOR]"] = top_3_behaviors[0]
        self.instance_data.content_parameters["[TOP_2_BEHAVIOR]"] = top_3_behaviors[1]
        self.instance_data.content_parameters["[TOP_3_BEHAVIOR]"] = top_3_behaviors[2]
        self.instance_data.content_parameters["[TOP_1_NEED]"] = top_3_needs[0]
        self.instance_data.content_parameters["[TOP_2_NEED]"] = top_3_needs[1]
        self.instance_data.content_parameters["[TOP_3_NEED]"] = top_3_needs[2]
        self.instance_data.content_parameters["[TOP_1_COMPETITOR]"] = top_2_competitors[0]
        self.instance_data.content_parameters["[TOP_2_COMPETITOR]"] = top_2_competitors[1]

        if self.instance_data.draft_parameters is None:
            self.instance_data.draft_parameters = {}

        self.instance_data.draft_parameters[UserObfuscator.AUDIENCE_ANGLE_ID_INT] = audience_angle_id_int
        self.instance_data.draft_parameters[UserObfuscator.APP_ID_INT] = app_id_int

        return {
            "all_apps": all_apps,
            "all_angles": all_angles,
        }

    def __get_follow_up_email_conference(self) -> str:
        sheet = PrivateDataModel.get_private_data()["google"]["google_docs"]["platform"]
        rows = external_api.read_sheet(sheet["sheet_id"], "Clients", to_arr_dict=True)
        for row in rows:
            if row["Email"] == self.email:
                conference = row["Conference"]
                if conference != "":
                    return f" at {conference}"
                break
        return ""

    def get_template_def(self):
        return FOLLOW_UP_EMAIL_ID

    def get_from_address(self):
        return "xxx@xxx.xxx"

    def get_creds(self):
        return "xxx@xxx.xxx"

    def get_content_parameters(self):
        return {
            "[CONFERENCE]": None,
            "[NAME]": None,
            "[MOST_DOWNLOADED_GAME]": None,
            "[LARGEST_AUDIENCE]": None,
            "[TOP_NON_AUDIENCE_GENRE]": None,
            "[TOP_NON_AUDIENCE_TOPIC]": None,
            "[TOP_DOMAIN]": None,
            "[TOP_1_BEHAVIOR]": None,
            "[TOP_2_BEHAVIOR]": None,
            "[TOP_3_BEHAVIOR]": None,
            "[TOP_1_NEED]": None,
            "[TOP_2_NEED]": None,
            "[TOP_3_NEED]": None,
            "[TOP_1_COMPETITOR]": None,
            "[TOP_2_COMPETITOR]": None
        }

    def __set_params(self):
        is_whitelisted = UserRegistrationHelper.get_whitelist_request_guid(
            conn=self.conn,
            email=self.email
        ) is not None

        if is_whitelisted:
            self.instance_data.content_parameters[
                "[MAIN_LINK_START]"] = self.get_registration_url()
            self.instance_data.content_parameters["[MAIN_LINK_END]"] = "</a>"
        self.instance_data.content_parameters["[INVITE_START]"] = ""
        self.instance_data.content_parameters["[INVITE_END]"] = ""

        user_data = self.__get_user_data_from_email()
        user_name = user_data["name"]
        arr = user_name.split(" ")
        self.instance_data.content_parameters["[NAME]"] = arr[0]
        self.instance_data.content_parameters["[CONFERENCE]"] = self.__get_follow_up_email_conference()

    def after_draft_loaded(self):
        self.__set_params()

    def set_email(self, email: str):
        super().set_email(email=email)
        self.__set_params()

    def modify_template_parameter_before_send(self, p: str, is_test_email=False) -> str:
        if p == "gb__content":
            is_whitelisted = UserRegistrationHelper.get_whitelist_request_guid(
                conn=self.conn,
                email=self.email
            ) is not None

            if not is_whitelisted:
                c = self.instance_data.template_parameters["gb__content"]
                ix1 = c.find("[INVITE_START]")
                ix2 = c.find("[INVITE_END]", ix1)
                if ix1 != -1 and ix2 != -1:
                    c = f"""{c[:ix1]}{c[ix2 + len("[INVITE_END]"):]}"""
                return c

        return super().modify_template_parameter_before_send(p, is_test_email=is_test_email)
