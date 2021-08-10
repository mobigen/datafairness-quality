import operator
import re
import configparser
import pandas as pd
import numpy as np
from collections import Counter
from dateutil.parser import parse
from transformers import ElectraTokenizer, ElectraForTokenClassification
from .ner_pipeline import NerPipeline

class ColumnStats:
    def __init__(self):
        self.column_name = ""
        self.row_count = 0
        self.column_type = ""
        self.column_pattern = ""
        self.missing_count = 0
        self.type_stats = {}
        self.pattern_stats = {}
        self.number_stats = {}
        self.string_stats = {}
        self.common_stats = {}
        self.quartile_stats = {}
        self.unique_stats = {}
        self.ner = ""


class DataQuality:
    def __init__(self, file_path):
        self._df = pd.read_csv(file_path, header=0, dtype=str)
        self.table_stats = {"column_stats": []}
        self.tokenizer = ElectraTokenizer.from_pretrained("monologg/koelectra-small-finetuned-naver-ner")
        self.model = ElectraForTokenClassification.from_pretrained("monologg/koelectra-small-finetuned-naver-ner")

    def set_regex(self):
        config = configparser.ConfigParser()
        config.optionxform = str  # config.ini 구문 분석 시, 대문자->소문자 변환되는 동작 비활성화
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
        if (
            regex_key == "ADDRESS"
            or regex_key == "URL"
            or regex_key == "TEXT_KOR"
            or regex_key == "TEXT_ENG"
        ):
            result = regex_compile.search(data)
        else:
            result = regex_compile.fullmatch(data)
        return result

    def predict_ner(self, column, column_name):
        ner = NerPipeline(model=self.model,
                        tokenizer=self.tokenizer,
                        ignore_special_tokens=True)

        answer, stats = ner.data_ner(column, column_name)
        return answer, stats

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

        for index, row in self._df.iterrows():
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
        type_stats = {"NUMBER": 0, "STRING": 0, "DATETIME": 0}

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
                try:
                    parse(data)
                    type_stats["DATETIME"] += 1
                except Exception:
                    type_stats["STRING"] += 1

        if type_stats["STRING"] >= 1:
            column_type = "STRING"
        elif (
            type_stats["NUMBER"] == 0
            and type_stats["STRING"] == 0
            and type_stats["DATETIME"] == 0
        ):
            column_type = None
        else:
            column_type = sorted(
                type_stats.items(), key=operator.itemgetter(1), reverse=True
            )[0][0]

        return column_type, type_stats, unique_stats, missing_cnt

    def calc_quartile(self, quantile, column):
        quartile_stats = {}
        column_df = pd.DataFrame({"col": column})
        quantile_data = pd.cut(column_df["col"], quantile)
        group_col = column_df["col"].groupby(quantile_data)

        for key, value in group_col.count().items():
            quartile_stats[
                key.__str__().replace(",", " ~").replace("]", "").replace("(", "")
            ] = value

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
                number_stats["mean"] = float(column.mean())
                number_stats["std"] = float(column.std())
                number_stats["median"] = float(np.median(column))

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
                string_stats["mean"] = np.mean(len_list)
                string_stats["std"] = np.std(len_list)
                string_stats["median"] = np.median(len_list)

            common_stats["mode"][Counter(column).most_common()[0][0]] = Counter(
                column
            ).most_common()[0][1]

        return number_stats, string_stats, common_stats, quartile_stats

    def calc_missing_rate(self, missing_cnt, row_cnt):
        try:
            return missing_cnt / row_cnt
        except ZeroDivisionError:
            return 0

    def calc_violation_rate(self, match_cnt, row_cnt):
        try:
            return (row_cnt - match_cnt) / row_cnt
        except ZeroDivisionError:
            return 0

    def calc_outlier_ratio(self, col_stats, column):
        outlier_cnt = 0
        if col_stats.column_type == "NUMBER":
            outlier_min = col_stats.number_stats["mean"] - (
                3 * col_stats.number_stats["std"]
            )
            outlier_max = col_stats.number_stats["mean"] + (
                3 * col_stats.number_stats["std"]
            )
            for data in column:
                if float(data) < outlier_min or outlier_max < float(data):
                    outlier_cnt += 1
        elif col_stats.column_type == "STRING":
            outlier_min = col_stats.string_stats["mean"] - (
                3 * col_stats.string_stats["std"]
            )
            outlier_max = col_stats.string_stats["mean"] + (
                3 * col_stats.string_stats["std"]
            )
            for data in column:
                if len(data) < outlier_min or outlier_max < len(data):
                    outlier_cnt += 1

        return outlier_cnt / col_stats.row_count

    def calc_uniqueness_violation_rate(self, row_cnt, unique_stats):
        uniqueness_violation_cnt = 0
        for value in unique_stats.values():
            if value >= 2:
                uniqueness_violation_cnt += value - 1

        return uniqueness_violation_cnt / row_cnt

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

        column_info["ner_entity"] = col_stats.ner

        column_info["row_count"] = col_stats.row_count
        column_info["missing_count"] = col_stats.missing_count

        sort_pattern = sorted(
            col_stats.pattern_stats.items(), key=lambda x: x[1], reverse=True
        )
        sort_type = sorted(
            col_stats.type_stats.items(), key=lambda x: x[1], reverse=True
        )

        column_info["pattern"] = {
            sort_pattern[index][0]: "{} ({}%)".format(
                sort_pattern[index][1],
                int((sort_pattern[index][1] / col_stats.row_count) * 100),
            )
            for index in range(len(sort_pattern))
        }

        column_info["type"] = {
            sort_type[index][0]: "{} ({}%)".format(
                sort_type[index][1],
                int((sort_type[index][1] / col_stats.row_count) * 100),
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
                    key: value for key, value in col_stats.quartile_stats.items()
                },
            }

        if len(col_stats.common_stats["mode"]) != 0:
            mode_key = list(col_stats.common_stats["mode"].keys())[0]
            column_info["mode_key"] = {
                "key": mode_key,
                "count": col_stats.common_stats["mode"].get(mode_key),
            }

        return column_info
