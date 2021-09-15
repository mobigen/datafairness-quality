from DQI import AutoDataQuality
from DQI import RuleDataQuality
from numpyencoder import NumpyEncoder
import json
import time

def Handler(req):
    start = time.time()
    iris_info = {
        'ADDR': '192.168.101.108',
        'USER_ID': 'fair',
        'PASSWD': '!cool@fairness#4',
        'DB_NAME': 'FAIR',
    }
    recv_msg = json.loads(req.input)
    mode = recv_msg["mode"]
    table_name = recv_msg["table_name"]
    try:
        if mode == "auto":
            auto = AutoDataQuality(None, iris_info, table_name) 
            data_dqi = auto.evaluation()
        elif mode == "rule":
            rule = RuleDataQuality(None, iris_info, table_name)
            data_dqi = rule.evaluation(recv_msg["rules"])
        result = {"result" : "SUCCESS", "data_dqi" : data_dqi}
        print(json.dumps(result, indent=3, cls=NumpyEncoder))
    except Exception as err:
        result = {"result" : "FAIL", "reason" : "{}".format(err)}
        print(json.dumps(result, indent=3, cls=NumpyEncoder))
    print("elapsed time : {}".format(time.time() - start))
    return str.encode(json.dumps(result, indent=3, ensure_ascii=False, cls=NumpyEncoder))

