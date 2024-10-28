from gembase_server_core.private_data.private_data_model import PrivateDataModel


class DmsConstants:
    platform_values_guid = PrivateDataModel.get_private_data()["google"]["google_docs"]["platform_values"]["dms_guid"]
    prompts_guid = PrivateDataModel.get_private_data()["google"]["google_docs"]["prompts"]["dms_guid"]
    platform_guid = PrivateDataModel.get_private_data()["google"]["google_docs"]["platform"]["dms_guid"]
    survey_v2_config = PrivateDataModel.get_private_data()["google"]["google_docs"]["survey_v2_config"]["dms_guid"]
    survey_v2_texts = PrivateDataModel.get_private_data()["google"]["google_docs"]["survey_v2_texts"]["dms_guid"]
