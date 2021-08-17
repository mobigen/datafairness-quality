import datetime as dt
import pandas as pd
import csv

import DQI.M6 as M6

class IRISDB():
    def __init__(self, iris_info):
        self.iris_info = iris_info

    def connect_db(self):
        try:
            self.conn = M6.Connection(
                addr_info=IRIS_INFO['ADDR'],
                id=IRIS_INFO['USER_ID'],
                password=IRIS_INFO['PASSWD'],
                Database=IRIS_INFO['DB_NAME'])
            assert isinstance(self.conn, M6.M6_Connection.Connection)
            self.cur = self.conn.cursor()
        except Exception as e:
            print(e)

    def insert_query(self, table_fields, insert_data):
        try:
            for data in insert_data:
                curtime = dt.datetime.now()
                sql = 'INSERT INTO {tbl_name} ({tbl_fields}) VALUES'.format(
                    tbl_name=IRIS_INFO['TBL_NAME'],
                    tbl_fields=', '.join(table_fields)
                    )

                print("SQL : {}".format(sql))
                sql_values = [
                    curtime.strftime("%Y%m%d"),
                    curtime.strftime("%Y%m%d%H%M%S")
                ]

                sql_values.extend(data)

                #print("BEFORE SQL VALUES : {}".format(sql_values))
                sql_values = ', '.join(list(map(lambda v: '\''+str(v)+'\'', sql_values)))
                #print("AFTER SQL VALUES : {}".format(sql_values))

                sql = sql + f' ( {sql_values} );'
                print(f"SQL: {' '.join(sql.split())}")
                res = self.cur.Execute2(sql)
                print(res)
        except Exception as e:
            print(e)

    def select_query(self):
        select_data = None
        meta = None
        try:
            res = self.cur.Execute2(f'SELECT * FROM {IRIS_INFO["TBL_NAME"]};')
            print(res)
            self.cur.ReadData()
            select_data = self.cur.buffer
            meta = self.cur.Metadata()
        except Exception as e:
            print(e)
        
        return meta["ColumnName"], select_data

    def __del__(self):
        self.cur.Close()
        self.conn.close()    


# Connection Info Example
IRIS_INFO = {
    'ADDR': '192.168.101.108',
    #'ADDR': '211.232.115.81',
    'USER_ID': 'fair',
    'PASSWD': '!cool@fairness#4',
    'DB_NAME': 'FAIR',
    'TBL_NAME': 'TBL_COMPANY'
}

TBL_FIELDS = [
    "PARTITION_ID",
    "PARTITION_DATE",
    "STATUS",
    "MESSAGE",
    "CORP_CODE",
    "CORP_NAME",
    "CORP_NAME_ENG",
    "STOCK_NAME",
    "STOCK_CODE",
    "CEO_NM",
    "CORP_CLS",
    "JURIR_NO",
    "BIZR_NO",
    "ADRES",
    "HM_URL",
    "IR_URL",
    "PHN_NO",
    "FAX_NO",
    "INDUTY_CODE",
    "EST_DT",
    "ACC_MT" 
]

def read_csv(file_path):
    insert_data = []
    with open(file_path) as fd:
        reader = csv.reader(fd)
        for line in reader:
            insert_data.append(line)
    return insert_data

if __name__ == "__main__":
    iris = IRISDB(IRIS_INFO)
    iris.connect_db()
    #iris.insert_query(TBL_FIELDS, insert_data[1:])
    meta, select_data = iris.select_query()
    df = pd.DataFrame(select_data, columns=meta)
    print("select result :\n {}".format(df))
    del iris