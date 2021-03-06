import operator
import re
import configparser
import pandas as pd
import numpy as np
from collections import Counter
from dateutil.parser import parse
from transformers import ElectraTokenizer, ElectraForTokenClassification
from .ner_pipeline import NerPipeline
from DB.IRISDB import IRISDB
from .bert import Ner


class ColumnStats:
    def __init__(self):
        self.column_name = None
        self.row_count = 0
        self.column_type = None
        self.column_pattern = None
        self.missing_count = 0
        self.type_stats = {}
        self.pattern_stats = {}
        self.number_stats = {}
        self.string_stats = {}
        self.common_stats = {}
        self.quartile_stats = {}
        self.unique_stats = {}
        self.ner = None
        self.time_distribution = None
        self.unknown = 0


class DataQuality:
    def __init__(self, file_path=None, db_info=None, table_name=None):
        if file_path != None:
            self._df = pd.read_csv(file_path, header=0, dtype=str)
        if table_name != None:
            self.iris = IRISDB(db_info)
            self.iris.connect_db()
            self.meta, select_data = self.iris.select_query(table_name)
            self._df = pd.DataFrame(select_data, columns=self.meta)
        self.db_info = db_info
        self.table_name = table_name

    def set_rule_for_db(self):
        table_list = ["REGEX", "REGEX_SET", "RANGE", "BIN_SET", "UNIQUE_SET"]
        regex = {}
        regex_compile = {}
        regex_set = {}
        unique_regex = []
        bin_regex = []
        range_info = {}

        db = IRISDB(self.db_info)
        db.connect_db()
        for table_name in table_list:
            _, select_data = db.select_query(table_name)
            for data in select_data:
                if table_name == "REGEX":
                    regex[data[0]] = data[1]
                elif table_name == "REGEX_SET":
                    regex_set[data[0]] = data[1].split(",")
                elif table_name == "RANGE":
                    range = data[1].split(",")
                    range_info[data[0]] = {
                        "min": int(range[0]),
                        "max": int(range[1])
                    }
                elif table_name == "BIN_SET":
                    bin_regex = data[0].split(",")
                elif table_name == "UNIQUE_SET":
                    unique_regex = data[0].split(",")

        del db
        for key, value in regex.items():
            regex_compile[key] = re.compile(value)
        return regex_compile, regex_set, unique_regex, bin_regex, range_info

    def set_regex(self):
        config = configparser.ConfigParser()
        config.optionxform = str  # config.ini ?????? ?????? ???, ?????????->????????? ???????????? ?????? ????????????
        config.read("conf/config.ini", encoding="utf-8")

        regex = {}
        regex_compile = {}
        regex_set = {}
        unique_regex = []
        bin_regex = []
        range_info = {}

        for pattern in config["REGEX"]:
            regex[pattern] = config["REGEX"][pattern]

        for set_name in config["REGEX_SET"]:
            regex_set[set_name] = config["REGEX_SET"][set_name].split(",")

        unique_regex = config["UNIQUE_SET"]["UNIQUE"].split(",")

        bin_regex = config["BIN_SET"]["BIN"].split(",")

        for pattern in config["RANGE"]:
            range = config["RANGE"][pattern].split(",")
            range_info[pattern] = {"min": int(range[0]), "max": int(range[1])}

        for key, value in regex.items():
            regex_compile[key] = re.compile(value)

        return regex_compile, regex_set, unique_regex, bin_regex, range_info

    def regex_match(self, regex_key, regex_compile, data):
        if (regex_key == "ADDRESS" or regex_key == "URL"
                or regex_key == "TEXT_KOR" or regex_key == "TEXT_ENG"):
            result = regex_compile.search(data)
        else:
            result = regex_compile.fullmatch(data)
        return result

    def predict_ko_ner(self, column):
        ner = NerPipeline(model=self.ko_model,
                          tokenizer=self.ko_tokenizer,
                          ignore_special_tokens=True)

        word_level_answer, ner_stats = ner.data_ner(column)
        return word_level_answer, ner_stats

    def predict_eng_ner(self, column):
        output = self.eng_model.predict(" ".join(column.tolist()))

        ner_stats = {}
        for res in output:
            entity = self.convert_eng_ner(res["tag"])
            if entity == None:
                continue
            if entity not in ner_stats:
                ner_stats[entity] = 0
            ner_stats[entity] += 1
        return ner_stats

    def convert_ko_ner(self, ner_entity):
        """
        ????????? ??????            ??????     ??????
        PERSON              PER     ??????, ?????? ??? ???????????? ?????? ?????? ???
        FIELD               FLD     ?????? ?????? ??? ??????, ??????, ?????? ???
        ARTIFACTS_WORKS     AFW     ???????????? ????????? ?????? ????????? ?????????
        ORGANIZATION        ORG     ?????? ??? ????????? ??????/????????? ?????? ??????
        LOCATION            LOC     ??????????????? ???????????? ?????? ???
        CIVILIZATION        CVL     ?????? ??? ????????? ????????? ??????
        DATE                DAT     ??????
        TIME                TIM     ??????
        NUMBER              NUM     ??????
        EVENT               EVT     ?????? ?????? ??? ?????? ????????? ?????? ???
        ANIMAL              ANM     ??????
        PLANT               PLT     ??????
        MATERIAL            MAT     ??????, ??????, ???????????? ???
        TERM                TRM     ?????? ??????, IT?????? ?????? ??? ?????? ????????? ??????

        {0: 'O', 1: 'PER-B', 2: 'PER-I', 3: 'FLD-B', 4: 'FLD-I', 5: 'AFW-B', 6: 'AFW-I',
        7: 'ORG-B', 8: 'ORG-I', 9: 'LOC-B', 10: 'LOC-I', 11: 'CVL-B', 12: 'CVL-I', 13: 'DAT-B',
        14: 'DAT-I', 15: 'TIM-B', 16: 'TIM-I', 17: 'NUM-B', 18: 'NUM-I', 19: 'EVT-B', 20: 'EVT-I',
        21: 'ANM-B', 22: 'ANM-I', 23: 'PLT-B', 24: 'PLT-I', 25: 'MAT-B', 26: 'MAT-I', 27: 'TRM-B', 28: 'TRM-I'}
        """
        result = ""
        if "PER" in ner_entity:
            result = "PERSON"
        elif "FLD" in ner_entity:
            result = "FIELD"
        elif "AFW" in ner_entity:
            result = "ARTIFACTS_WORKS"
        elif "ORG" in ner_entity:
            result = "ORGANIZATION"
        elif "LOC" in ner_entity:
            result = "LOCATION"
        elif "CVL" in ner_entity:
            result = "CIVILIZATION"
        elif "DAT" in ner_entity:
            result = "DATE"
        elif "TIM" in ner_entity:
            result = "TIME"
        elif "NUM" in ner_entity:
            result = "NUMBER"
        elif "EVT" in ner_entity:
            result = "EVENT"
        elif "ANM" in ner_entity:
            result = "ANIMAL"
        elif "PLT" in ner_entity:
            result = "PLANT"
        elif "MAT" in ner_entity:
            result = "MATERIAL"
        elif "TRM" in ner_entity:
            result = "TERM"

        return result

    def convert_eng_ner(self, ner_entity):
        '''
            O	Outside of a named entity
            B-MIS	Beginning of a miscellaneous entity right after another miscellaneous entity
            I-MIS	Miscellaneous entity
            B-PER	Beginning of a person???s name right after another person???s name
            I-PER	Person???s name
            B-ORG	Beginning of an organization right after another organization
            I-ORG	organization
            B-LOC	Beginning of a location right after another location
            I-LOC	Location
        '''
        result = None
        if "PER" in ner_entity:
            result = "PERSON"
        elif "MIS" in ner_entity:
            result = "MIS"
        elif "ORG" in ner_entity:
            result = "ORGANIZATION"
        elif "LOC" in ner_entity:
            result = "LOCATION"

        return result

    def get_ner(self, col_stats, column, column_name, text_kor_set):
        f_text_kor = 0
        for pattern in col_stats.pattern_stats.keys():
            if pattern in text_kor_set:
                f_text_kor = 1
                break
        ner = None
        if f_text_kor == 1 or col_stats.column_type == "NUMBER":
            print("GET ko NER ({})".format(column_name))
            _, stats = self.predict_ko_ner(column)
            #if len(stats[column_name]) == 0:
            if len(stats) == 0:
                pass
            else:
                mod_ner = sorted(stats.items(),
                                 key=operator.itemgetter(1),
                                 reverse=True)[0][0]
                ner = self.convert_ko_ner(mod_ner)
        else:
            print("GET eng NER ({})".format(column_name))
            stats = self.predict_eng_ner(column)
            if len(stats) == 0:
                pass
            else:
                mod_ner = sorted(stats.items(),
                                 key=operator.itemgetter(1),
                                 reverse=True)[0][0]
                ner = self.convert_eng_ner(mod_ner)
        return ner

    def cal_row_missing_rate(self):
        rate_percentile = {
            "0.0": 0,
            "0.1": 0,
            "0.2": 0,
            "0.3": 0,
            "0.4": 0,
            "0.5": 0,
            "0.6": 0,
            "0.7": 0,
            "0.8": 0,
            "0.9": 0,
            "1.0": 0,
        }

        for _, row in self._df.iterrows():
            missing_cnt = 0
            row = np.where(row.isnull(), None, row)
            for cell in row:
                if cell == None:
                    missing_cnt += 1
            missing_rate = missing_cnt / len(row)
            for key in rate_percentile.keys():
                if float(key) >= missing_rate:
                    rate_percentile[key] += 1
                    break

        return rate_percentile

    def check_credit_no(self, credit_no):
        credit_no = credit_no.replace(" ", "").replace("-", "")
        sum = 0

        for index, value in enumerate(credit_no[:15]):
            if index % 2 == 0:
                if 2 * int(value) >= 10:
                    temp_value = str(2 * int(value))
                    sum += int(temp_value[0]) + int(temp_value[1])
                else:
                    sum += 2 * int(value)
            else:
                sum += int(value)

        if (sum + int(credit_no[15])) % 10 == 0:
            return True
        # else:
        #    print("Invalid Credit Number [{}]".format(credit_no))
        return False

    def check_corp_no(self, corp_no):
        corp_no = corp_no.replace(" ", "").replace("-", "")
        sum = 0
        for index, value in enumerate(corp_no[:12]):
            if index % 2 == 0:
                sum += int(value) * 1
            else:
                sum += int(value) * 2

        valid_no = 10 - (sum % 10)
        if valid_no == 10:
            valid_no = 0

        if int(corp_no[12]) == valid_no:
            return True
        # else:
        #    print("Invalid Corporate Registration Number [{}]".format(corp_no))
        return False

    def check_valid(self, regex_key, data):
        result = True
        if regex_key == "CREDIT_NO":
            result = self.check_credit_no(data)
        elif regex_key == "CORP_NO":
            result = self.check_corp_no(data)
        return result

    def check_type(self, column):
        missing_cnt = 0
        type_stats = {"NUMBER": 0, "STRING": 0}  # for Type Missmatch Rate
        unique_stats = {}

        for data in column:
            if data == None:
                missing_cnt += 1
                continue

            if data not in unique_stats:
                unique_stats[data] = 1
            else:
                unique_stats[data] += 1

            try:
                float(data)
                type_stats["NUMBER"] += 1
            except:
                type_stats["STRING"] += 1

        if type_stats["NUMBER"] == 0 and type_stats["STRING"] == 0:
            column_type = None
        elif type_stats["STRING"] >= 1:
            column_type = "STRING"
        else:
            column_type = "NUMBER"

        return column_type, type_stats, unique_stats, missing_cnt

    """
        time_offset
        "Y" : year, "M" : month, "W" : week, "D" : day, "H" : hour, "T" : minute
    """

    def get_time_distribution(self, df, column_name, time_offset="Y"):
        time_distribution = {}
        try:
            df[column_name] = pd.to_datetime(df[column_name])
            df.set_index(column_name, drop=False, inplace=True)

            time_info = df.resample(time_offset)[column_name].count()
            for time in time_info.index:
                if time_offset == "Y":
                    time_distribution[str(time)[:4]] = time_info.loc[time]
                elif time_offset == "M":
                    time_distribution[str(time)[:7]] = time_info.loc[time]
                elif time_offset == "D":
                    time_distribution[str(time)[:10]] = time_info.loc[time]
                elif time_offset == "H":
                    time_distribution[str(time)[:13]] = time_info.loc[time]
                elif time_offset == "T":
                    time_distribution[str(time)[:16]] = time_info.loc[time]
        except:
            time_distribution = None

        return time_distribution

    def calc_quartile(self, quantile, column):
        quartile_stats = {}
        column_df = pd.DataFrame({"col": column})
        quantile_data = pd.cut(column_df["col"], quantile)
        group_col = column_df["col"].groupby(quantile_data)

        for key, value in group_col.count().items():
            quartile_stats[key.__str__().replace(",", " ~").replace(
                "]", "").replace("(", "")] = value

        return quartile_stats

    def calc_statistics(self, column_type, quartile, column):
        number_stats = {"mean": 0, "min": 0, "max": 0, "median": 0, "std": 0}
        string_stats = {"mean": 0, "min": {}, "max": {}, "median": 0, "std": 0}
        common_stats = {"mode": {}}
        quartile_stats = {}
        if len(column) != 0:
            if column_type == "NUMBER":
                column = np.array(column, dtype=np.float64)
                number_stats["min"] = float(column.min())
                number_stats["max"] = float(column.max())
                number_stats["mean"] = round(float(column.mean()), 5)
                number_stats["std"] = round(float(column.std()), 5)
                number_stats["median"] = round(float(np.median(column)), 5)

                quartile_stats = self.calc_quartile(quartile, column)
            elif column_type == "STRING":
                len_list = [len(x) for x in column]

                string_stats["min"] = {
                    "key": column[len_list.index(min(len_list))],
                    "len": min(len_list),
                }
                string_stats["max"] = {
                    "key": column[len_list.index(max(len_list))],
                    "len": max(len_list),
                }
                string_stats["mean"] = round(np.mean(len_list), 5)
                string_stats["std"] = round(np.std(len_list), 5)
                string_stats["median"] = round(np.median(len_list), 5)

            common_stats["mode"][Counter(column).most_common()[0]
                                 [0]] = Counter(column).most_common()[0][1]

        return number_stats, string_stats, common_stats, quartile_stats

    def calc_missing_rate(self, missing_cnt, row_cnt):
        try:
            return (missing_cnt / row_cnt) * 100
        except ZeroDivisionError:
            return 0

    def calc_violation_rate(self, match_cnt, row_cnt):
        try:
            return ((row_cnt - match_cnt) / row_cnt) * 100
        except ZeroDivisionError:
            return 0

    def calc_outlier_ratio(self, col_stats, column):
        outlier_cnt = 0
        if col_stats.column_type == "NUMBER":
            outlier_min = col_stats.number_stats["mean"] - (
                3 * col_stats.number_stats["std"])
            outlier_max = col_stats.number_stats["mean"] + (
                3 * col_stats.number_stats["std"])
            for data in column:
                if float(data) < outlier_min or outlier_max < float(data):
                    outlier_cnt += 1
        elif col_stats.column_type == "STRING":
            outlier_min = col_stats.string_stats["mean"] - (
                3 * col_stats.string_stats["std"])
            outlier_max = col_stats.string_stats["mean"] + (
                3 * col_stats.string_stats["std"])
            for data in column:
                if len(data) < outlier_min or outlier_max < len(data):
                    outlier_cnt += 1

        return (outlier_cnt / col_stats.row_count) * 100

    def calc_uniqueness_violation_rate(self, col_stats):
        uniqueness_violation_cnt = 0
        for value in col_stats.unique_stats.values():
            if value >= 2:
                uniqueness_violation_cnt += value - 1

        return (uniqueness_violation_cnt / col_stats.row_count) * 100

    def get_quartile(self, unique_data_cnt):
        max_quartile = 10
        quartile = unique_data_cnt / 2
        if quartile <= 1:
            quartile = 1
        if quartile > max_quartile:
            quartile = max_quartile

        return int(quartile)

    def make_col_info(
        self,
        col_stats,
    ):
        column_info = {}

        column_info["column_name"] = col_stats.column_name
        column_info["column_type"] = col_stats.column_type

        if col_stats.row_count == col_stats.missing_count:
            column_info["column_pattern"] = None
        else:
            column_info["column_pattern"] = col_stats.column_pattern

        column_info["named_entity_recognition"] = col_stats.ner

        column_info["row_count"] = col_stats.row_count
        column_info["missing_count"] = col_stats.missing_count

        sort_pattern = sorted(col_stats.pattern_stats.items(),
                              key=lambda x: x[1],
                              reverse=True)
        sort_type = sorted(col_stats.type_stats.items(),
                           key=lambda x: x[1],
                           reverse=True)

        column_info["pattern"] = {
            sort_pattern[index][0]: "{} ({:.3f}%)".format(
                sort_pattern[index][1],
                float((sort_pattern[index][1] / col_stats.row_count) * 100),
            )
            for index in range(len(sort_pattern))
        }

        column_info["type"] = {
            sort_type[index][0]: "{} ({:.3f}%)".format(
                sort_type[index][1],
                float((sort_type[index][1] / col_stats.row_count) * 100),
            )
            for index in range(len(sort_type))
        }

        if col_stats.column_type == "STRING":
            column_info["string_stat"] = {
                "len mean": col_stats.string_stats["mean"],
                "len std": col_stats.string_stats["std"],
                "len median": col_stats.string_stats["median"],
                "len min": col_stats.string_stats["min"],
                "len max": col_stats.string_stats["max"],
            }
        elif col_stats.column_type == "NUMBER":
            column_info["number_stat"] = {
                "min": col_stats.number_stats["min"],
                "max": col_stats.number_stats["max"],
                "mean": col_stats.number_stats["mean"],
                "std": col_stats.number_stats["std"],
                "median": col_stats.number_stats["median"],
                "quartile": {
                    key: value
                    for key, value in col_stats.quartile_stats.items()
                },
            }

        if col_stats.time_distribution != None:
            column_info["time_distrinution"] = col_stats.time_distribution

        if len(col_stats.common_stats["mode"]) != 0:
            mode_key = list(col_stats.common_stats["mode"].keys())[0]
            column_info["mode_key"] = {
                "key": mode_key,
                "count": col_stats.common_stats["mode"].get(mode_key),
            }

        return column_info
