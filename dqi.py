from DQI import AutoDataQuality
from DQI import RuleDataQuality
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
    
    result = auto_dqi(db_info=iris_info, table_name = "DQI_DATA_5000")
    #result = auto_dqi(file_path=file_path, db_info=iris_info)
    
    #result = rule_dqi(file_path=file_path, db_info=iris_info)
    #result = rule_dqi(db_info=iris_info, table_name = "DQI_DATA")

    print(json.dumps(result, indent=3, ensure_ascii=False))

if __name__ == "__main__":
    dqi_test()
    