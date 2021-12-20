from time import time
from DQI import AutoDataQuality
from DQI import RuleDataQuality
from DQI import DataQuality
from DQI import Correction
from numpyencoder import NumpyEncoder
import json


def auto_dqi(file_path=None, db_info=None, table_name=None):
    auto = AutoDataQuality(file_path, db_info, table_name)
    #f_ner = "on"
    result = auto.evaluation()

    return result


def rule_dqi(file_path=None, db_info=None, table_name=None):
    # rule_path = "conf/rule.json"
    rule_path = "conf/rule_for_db.json"

    with open(rule_path, "r") as fd:
        rules = json.load(fd)

    rule = RuleDataQuality(file_path, db_info, table_name)
    #f_ner = "on"
    result = rule.evaluation(rules)

    return result


def data_correction(file_path=None, db_info=None, table_name=None):
    correction = Correction(file_path, db_info, table_name)
    #correction.run_correction(table_name, "ACC_MT", "pattern_mismatch_rate",
    #                          "imputation", "DATETIME", "NUMBER")
    #correction.run_correction(table_name, "HM_URL", "type_missmatch_rate",
    #                          "imputation", "URL", "STRING")
    #correction.run_correction(table_name, "BIZR_NO", "type_missmatch_rate", "imputation",
    #                          "BIZ_NO", "NUMBER")
    correction.run_correction("NEW_TABLE", "HM_URL", "missing_rate", "delete",
                              "URL", "STRING")


def get_column_name(file_path=None, db_info=None, table_name=None):
    get_column_name = DataQuality(file_path, db_info, table_name)
    column_names = []
    for column_name in get_column_name._df.columns:
        column_names.append(column_name)
    result = {"result": "SUCCESS", "column_name": column_names}

    return result


def dqi_test():
    file_path = "sample_data/company_1000.csv"

    iris_info = {
        "ADDR": "192.168.101.108",
        "USER_ID": "fair",
        "PASSWD": "!cool@fairness#4",
        "DB_NAME": "FAIR",
    }
    # result = get_column_name(db_info=iris_info, table_name="DQI_MOVIES")
    result = auto_dqi(db_info=iris_info,
                      table_name="PATTERN_JURIR_NO_DQI_FOR_OFFICIAL_TEST")
    # result = auto_dqi(file_path=file_path, db_info=iris_info)

    # result = rule_dqi(file_path=file_path, db_info=iris_info)
    # result = rule_dqi(db_info=iris_info, table_name="DQI_COMPANY_5000")
    print(json.dumps(result, indent=3, ensure_ascii=False, cls=NumpyEncoder))

    #data_correction(db_info=iris_info, table_name="DQI_FOR_OFFICIAL_TEST")


import time

if __name__ == "__main__":
    start = time.time()
    dqi_test()
    print("elapsed time : {}".format(time.time() - start))
