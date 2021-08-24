from DB.IRISDB import IRISDB
import configparser

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
        db.insert_query(table_name, column_info, insert_data)
        del db

if __name__ == "__main__":
    rule_insert()
