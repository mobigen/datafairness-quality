import csv
from DB.IRISDB import IRISDB


def read_csv(file_path):
    insert_data = []
    with open(file_path) as fd:
        reader = csv.reader(fd)
        for line in reader:
            insert_data.append(line)
    return insert_data


company_fields = [
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
    "ACC_MT",
]
company_data = (
    "/Users/cbc/DEV/Mobigen/datafairness-quality/sample_data/company_5000.csv"
)


transfers_fields = ["URL", "ID", "FEDERATION", "FORM_FED", "TRANSFER_DATE"]
transfers_data = "/Users/cbc/DEV/Mobigen/datafairness-quality/sample_data/transfers.csv"


biopics_fields = [
    "TITLE",
    "SITE",
    "COUNTRY",
    "YEAR_RELEASE",
    "BOX_OFFICE",
    "DIRECTOR",
    "NUMBER_OF_SUBJECTS",
    "SUBJECT",
    "TYPE_OF_SUBJECT",
    "RACE_KNOWN",
    "SUBJECT_RACE",
    "PERSON_OF_COLOR",
    "SUBJEC_SEX",
    "LEAD_ACTOR_ACTRESS",
]
biopics_data = "/Users/cbc/DEV/Mobigen/datafairness-quality/sample_data/biopics.csv"


movies_fields = [
    "YEAR",
    "IMDB",
    "TITLE",
    "TEST",
    "CLEAN_TEST",
    "BINARY",
    "BUDGET",
    "DOMGROSS",
    "INTGROSS",
    "CODE",
    "BUDGET_2013",
    "DOMGROSS_2013",
    "INTGROSS_2013",
    "PERIOD_CODE",
    "DECADE_CODE",
]
movies_data = "/Users/cbc/DEV/Mobigen/datafairness-quality/sample_data/movies.csv"

data_list = {
    "company": {
        "table_name": "DQI_COMPANY_5000",
        "fields": company_fields,
        "data": company_data,
    },
    "transfers": {
        "table_name": "DQI_TRANSFERS",
        "fields": transfers_fields,
        "data": transfers_data,
    },
    "biopics": {
        "table_name": "DQI_BIOPICS",
        "fields": biopics_fields,
        "data": biopics_data,
    },
    "movies": {
        "table_name": "DQI_MOVIES",
        "fields": movies_fields,
        "data": movies_data,
    },
}


def data_insert():
    iris_info = {
        "ADDR": "192.168.101.108",
        "USER_ID": "fair",
        "PASSWD": "!cool@fairness#4",
        "DB_NAME": "FAIR",
    }
    data_name = "company"
    insert_data = read_csv(data_list[data_name]["data"])
    iris = IRISDB(iris_info)
    iris.connect_db()
    iris.insert_query(
        data_list[data_name]["table_name"],
        data_list[data_name]["fields"],
        insert_data[1:],
    )
    del iris


if __name__ == "__main__":
    data_insert()
