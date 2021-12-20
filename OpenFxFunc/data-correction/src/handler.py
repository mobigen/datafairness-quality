from DQI import Correction
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
    data_index = recv_msg["data_index"]
    table_name = recv_msg["table_name"]
    correction = recv_msg["correction"]
    column_name = recv_msg["column_name"]
    column_pattern = recv_msg["column_pattern"]
    column_type = recv_msg["column_type"]
    print("data_index : {}\ntable_name : {}\ncorrection : {}\ncolumn_name : {}\ncolumn_pattern : {}\ncolumn_type : {}".format(data_index, table_name, correction, column_name, column_pattern, column_type))
    try:
        correcte = Correction(None, iris_info, table_name)
        table_name = correcte.run_correction(table_name, column_name, data_index, correction, column_pattern, column_type)
        result = {"result": "SUCCESS", "correction_table_name" : table_name}
        print(json.dumps(result, indent=3, cls=NumpyEncoder))
    except Exception as err:
        result = {"result" : "FAIL", "reason" : "{}".format(err)}
        print(json.dumps(result, indent=3, cls=NumpyEncoder))
    print("elapsed time : {}".format(time.time() - start))
    return str.encode(json.dumps(result, indent=3, ensure_ascii=False, cls=NumpyEncoder))
