from RULE import DQI_RULE
import json

def read_rule():
    recv_rule = None
    with open("RULE/regex_rule.json", "r") as fd:
        recv_rule = json.load(fd)
    return recv_rule


if __name__ == "__main__":
    recv_rule = read_rule()

    db_info = {
        "ADDR": "192.168.101.108",
        #'ADDR': '211.232.115.81',
        "USER_ID": "fair",
        "PASSWD": "!cool@fairness#4",
        "DB_NAME": "FAIR",
    }
    rule_db = DQI_RULE(db_info=db_info, mode="DB")
    #rule_db = DQI_RULE(rule_path="test.ini", mode="FILE")
    #rule_db.set_rule(recv_rule)
    #rule_db.delete_rule(recv_rule)
    rule_db.display_rule()
    del rule_db