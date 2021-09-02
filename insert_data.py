import csv
from DB.IRISDB import IRISDB

def read_csv(file_path):
     insert_data = []
     with open(file_path) as fd:
         reader = csv.reader(fd)
         for line in reader:
             insert_data.append(line)
     return insert_data


tbl_fields = [
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
def data_insert():
    iris_info = {
        'ADDR': '192.168.101.108',
        #'ADDR': '211.232.115.81',
        'USER_ID': 'fair',
        'PASSWD': '!cool@fairness#4',
        'DB_NAME': 'FAIR',
    }
    insert_data = read_csv("/Users/cbc/DEV/Mobigen/datafairness-quality/sample_data/company_5000.csv")
    iris = IRISDB(iris_info)
    iris.connect_db()
    iris.insert_query("DQI_DATA_5000", tbl_fields, insert_data[1:])
    del iris

if __name__ == "__main__":
    data_insert()
