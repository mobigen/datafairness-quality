from DQI import AutoDataQuality
from DQI import RuleDataQuality
from DQI import DataQuality
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
    f_ner = "off"

    try:
        f_ner = recv_msg["ner"]
    except Exception as err:
        print(err)

    try:
        if mode == "auto":
            auto = AutoDataQuality(None, iris_info, table_name) 
            data_dqi = auto.evaluation(f_ner)
            result = {"result" : "SUCCESS", "data_dqi" : data_dqi}
        elif mode == "rule":
            rule = RuleDataQuality(None, iris_info, table_name)
            data_dqi = rule.evaluation(recv_msg["rules"], f_ner)
            result = {"result" : "SUCCESS", "data_dqi" : data_dqi}
        elif mode == "get_name":
            column_names = []
            get_column_name = DataQuality(None, iris_info, table_name)
            for column_name in get_column_name._df.columns:
                column_names.append(column_name)
            result = {"result": "SUCCESS", "column_name" : column_names}
        print(json.dumps(result, indent=3, cls=NumpyEncoder))
    except Exception as err:
        result = {"result" : "FAIL", "reason" : "{}".format(err)}
        print(json.dumps(result, indent=3, cls=NumpyEncoder))
    print("elapsed time : {}".format(time.time() - start))
    return str.encode(json.dumps(result, indent=3, ensure_ascii=False, cls=NumpyEncoder))

