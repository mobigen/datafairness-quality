from DQI import AutoDataQuality
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
    try:
        recv_msg = json.loads(req.input)
        mode = recv_msg["mode"]
        table_name = recv_msg["table_name"]

        auto = AutoDataQuality(None, iris_info, table_name)
        if mode == "dqi":
            data_dqi = auto.evaluation()
        elif mode == "correction":
            column_name = recv_msg["column_name"]
            correction = recv_msg["correction"]
            data_dqi = auto.data_quality_correction(column_name, correction, "row_delete")
        result = {"result" : "SUCCESS", "data_dqi" : data_dqi}
        print(json.dumps(result, indent=3, cls=NumpyEncoder))
    except Exception as err:
        result = {"result" : "FAIL", "reason" : "{}".format(err)}
        print(json.dumps(result, indent=3, cls=NumpyEncoder))
    print("elapsed time : {}".format(time.time() - start))
    return str.encode(json.dumps(result, indent=3, ensure_ascii=False, cls=NumpyEncoder))

