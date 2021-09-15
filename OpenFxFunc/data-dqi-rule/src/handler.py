from RULE import DQI_RULE
import json

def Handler(req):
    iris_info = {
        "ADDR": "192.168.101.108",
        "USER_ID": "fair",
        "PASSWD": "!cool@fairness#4",
        "DB_NAME": "FAIR",
    }
    recv_msg = json.loads(req.input)
    action = recv_msg["action"]

    try: 
        rule_db = DQI_RULE(db_info=iris_info, mode="DB")
        
        if action == "create":
            rule_db.set_rule(recv_msg)
            result = {"result" : "SUCCESS"}
        elif action == "delete":
            rule_db.delete_rule(recv_msg)
            result = {"result" : "SUCCESS"}
        elif action == "display":
            dis_rule = rule_db.display_rule()
            result = {"result" : "SUCCESS", "rules" : dis_rule}
    except Exception as err:
        result = {"result" : "FAIL", "reason" : "{}".format(err)}
    return str.encode(json.dumps(result, indent=3, ensure_ascii=False))

