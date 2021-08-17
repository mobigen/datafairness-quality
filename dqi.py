from DQI import AutoDataQuality
from DQI import RuleDataQuality
import json

def auto_dqi(file_path=None, db_info=None):
    auto = AutoDataQuality(file_path, db_info)
    result = auto.evaluation()

    return result

def rule_dqi(file_path=None, db_info=None):
    with open("conf/rule.json", "r") as fd:
        rules = json.load(fd)

    rule = RuleDataQuality(file_path, db_info)
    result = rule.evaluation(rules)

    return result

if __name__ == "__main__":
    file_path = "sample_data/company_100.csv"

    iris_info = {
        'ADDR': '192.168.101.108',
        #'ADDR': '211.232.115.81',
        'USER_ID': 'fair',
        'PASSWD': '!cool@fairness#4',
        'DB_NAME': 'FAIR',
        'TBL_NAME': 'TBL_COMPANY'
    }

    #result = auto_dqi(db_info=iris_info)
    result = auto_dqi(file_path=file_path)
    
    #result = rule_dqi(file_path=file_path)
    #result = rule_dqi(db_info=iris_info)

    print(json.dumps(result, indent=3, ensure_ascii=False))