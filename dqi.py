from re import split
from DQI import AutoDataQuality
from DQI import RuleDataQuality
from DQI import IRISDB
import configparser
import json

def auto_dqi(file_path=None, db_info=None, table_name=None):
    auto = AutoDataQuality(file_path, db_info, table_name)
    result = auto.evaluation()

    return result

def rule_dqi(file_path=None, db_info=None, table_name=None):
    #rule_path = "conf/rule.json"
    rule_path = "conf/rule_for_db.json"

    with open(rule_path, "r") as fd:
        rules = json.load(fd)

    rule = RuleDataQuality(file_path, db_info, table_name)
    result = rule.evaluation(rules)

    return result

def dqi_test():
    file_path = "sample_data/company_100.csv"

    iris_info = {
        'ADDR': '192.168.101.108',
        #'ADDR': '211.232.115.81',
        'USER_ID': 'fair',
        'PASSWD': '!cool@fairness#4',
        'DB_NAME': 'FAIR',
    }

    #result = auto_dqi(db_info=iris_info, table_name = "TBL_COMPANY")
    #result = auto_dqi(file_path=file_path, db_info=iris_info)
    
    #result = rule_dqi(file_path=file_path, db_info=iris_info)
    result = rule_dqi(db_info=iris_info, table_name = "TBL_COMPANY")

    print(json.dumps(result, indent=3, ensure_ascii=False))

def rule_insert():
    config = configparser.ConfigParser()
    config.optionxform = str  # config.ini 구문 분석 시, 대문자->소문자 변환되는 동작 비활성화
    config.read("conf/config.ini", encoding="utf-8")
    
    table_list = ["REGEX", "REGEX_SET", "RANGE", "BIN_SET", "UNIQUE_SET"]
    column_list = [["NAME", "EXPRESSION"], ["NAME", "REGEX_NAME"], ["NAME", "RANGE"], ["SET_NAME"], ["SET_NAME"]]

    for table_name, column_info in zip(table_list, column_list):
        insert_data = []

        if table_name == "BIN_SET":        
            insert_data.append(insert_data.append([config[table_name]["BIN"]]))
        elif table_name == "UNIQUE_SET":        
            insert_data.append(insert_data.append([config[table_name]["UNIQUE"]]))
        else:
            for key, value in config[table_name].items():
                tmp = [key, value]
                insert_data.append(tmp) 

        iris_info = {
            'ADDR': '192.168.101.108',
            #'ADDR': '211.232.115.81',
            'USER_ID': 'fair',
            'PASSWD': '!cool@fairness#4',
            'DB_NAME': 'FAIR',
        }
        db = IRISDB(iris_info)
        db.connect_db()
        db.insert_query_for_global(table_name, column_info, insert_data)
        del db

if __name__ == "__main__":
    #rule_insert()
    dqi_test()
