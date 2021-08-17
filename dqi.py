from DQI import AutoDataQuality
from DQI import RuleDataQuality
import json

def auto_dqi(file_path):
    auto = AutoDataQuality(file_path)
    result = auto.evaluation()

    return result

def rule_dqi(file_path):
    with open("conf/rule.json", "r") as fd:
        rules = json.load(fd)

    rule = RuleDataQuality(file_path)
    result = rule.evaluation(rules)

    return result

if __name__ == "__main__":
    file_path = "sample_data/company_100.csv"

    result = auto_dqi(file_path)
    
    #result = rule_dqi(file_path)

    print(json.dumps(result, indent=3, ensure_ascii=False))