from DataQuality import DataQuality
from DataQuality import ColumnStats
import numpy as np
import json


class RuleDataQuailty(DataQuality):
    def __init__(self, file_path):
        super().__init__(file_path)

    def _set_rule(self, regex_set, rules):
        column_rule = {}

        for rule in rules["rules"]:
            column_name = rule["column"]
            set_name = rule["rule"]

            if len(column_name) == 0:
                print("column name is empty.\n{}".format(rule))
                return None

            if len(set_name) == 0:
                print("rule name is empty.\n{}".format(rule))
                return None

            if set_name != "STATS" and set_name not in regex_set.keys():
                print("undefined regex name. ({})".format(set_name))
                return None
            
            column_rule[column_name] = set_name

        return column_rule

    def _check_range(self, column, pattern_name, range_info):
        pattern_stats = {"MATCH": 0, "MISSMATCH": 0, "INVALID": 0}
        min = range_info[pattern_name]["min"]
        max = range_info[pattern_name]["max"]

        for data in column:
            if data == None:
                continue
            try:
                data = float(data)

                if min <= data and data <= max:
                    pattern_stats["MATCH"] += 1
                else:
                    pattern_stats["MISSMATCH"] += 1
            except Exception as Err:
                pattern_stats["INVALID"] += 1

        return pattern_stats

    def _check_pattern(self, column, column_name, column_rule, regex_set, regex_compile):
        pattern_stats = {"INVALID" : 0, "UNKNOWN" : 0}
        set_name = column_rule[column_name]

        for key, value in column_rule.items():
            print("key : {}, value : {}".format(key, value))
        '''
        for data in column:
            if data == None:
                continue

            f_find = 0
            for key, value in regex_compile.items():
                result = self._regex_match(key, value, data)

                if result != None:
                    if self._check_valid(key, data) == False:
                        break
                    pattern_stats[key] += 1
                    f_find = 1

            if f_find == 0:
                if "UNKNOWN" not in pattern_stats:
                    pattern_stats["UNKNOWN"] = 0
                pattern_stats["UNKNOWN"] += 1

        column_pattern = sorted(
            pattern_stats.items(), key=operator.itemgetter(1), reverse=True
        )[0][0]

        return column_pattern
        '''
        if set_name == "STATS":
            return pattern_stats

        for data in column:
            if data == None:
                continue

            for pattern_name in regex_set[set_name]:
                print("pattern_name : {}".format(pattern_name))

                result = self._regex_match(pattern_name, regex_compile[pattern_name], data)
                if result != None:
                    if self._check_valid(pattern_name, data) == False:
                        pattern_stats["INVALID"] += 1
                        continue
                    if pattern_name not in pattern_stats:
                        pattern_stats[pattern_name] = 0
                    pattern_stats[pattern_name] += 1
                else:
                    pattern_stats["UNKNOWN"] += 1

        return pattern_stats

    def _calc_col_dqi(
        self, column_info, column, col_stats, unique_regex, bin_regex, range_info
    ):
        data_dqi = {}

        max_type_cnt = sorted(
            col_stats.type_stats.items(), key=lambda x: x[1], reverse=True
        )[0][1]

        data_dqi["missing_rate"] = self._calc_missing_rate(
            column_info["missing_count"], column_info["row_count"]
        )

        data_dqi["type_missmatch_rate"] = self._calc_violation_rate(
            max_type_cnt, column_info["row_count"]
        )

        #data_dqi["pattern_mismatch_rate"] = self._calc_violation_rate(
        #    col_stats.pattern_stats["MATCH"], column_info["row_count"]
        #)
        #data_dqi["consistency_violation_rate"] = data_dqi["pattern_mismatch_rate"]
        data_dqi["outlier_ratio"] = self._calc_outlier_ratio(column_info, column)

        if column_info["column_pattern"] in unique_regex:
            data_dqi[
                "uniqueness_violation_rate"
            ] = self._calc_uniqueness_violation_rate(
                column_info["row_count"], col_stats
            )

        #if column_info["column_pattern"] in bin_regex:
        #    data_dqi["binary_violation_rate"] = self._calc_violation_rate(
        #        col_stats.pattern_stats["MATCH"], column_info["row_count"]
        #    )

        #if column_info["column_pattern"] in range_info:
        #    data_dqi["range_violation_rate"] = self._calc_violation_rate(
        #        col_stats.pattern_stats["MATCH"], column_info["row_count"]
        #    )

        return data_dqi

    def evaluation(self, rules):
        table_dqi = {}

        regex, regex_compile, regex_set, unique_regex, bin_regex, range_info = self._set_regex()

        column_rule = self._set_rule(regex_set, rules)
        if column_rule == None:
            return None

        for column_name in self._df.columns:
            col_stats = ColumnStats()
            col_stats.column_name = column_name
            col_stats.column_pattern = column_rule[column_name]
            col_stats.row_count = len(self._df[column_name])

            column = np.where(
                self._df[column_name].isnull(), None, self._df[column_name]
            )

            if col_stats.column_pattern in range_info:
                col_stats.pattern_stats = self._check_range(
                    column, col_stats.column_pattern, range_info
                )
            else:
                col_stats.pattern_stats = self._check_pattern(
                    column, column_name, column_rule, regex_set, regex_compile
                )

            (
                col_stats.column_type,
                col_stats.type_stats,
                col_stats.unique_stats,
                col_stats.missing_cnt,
            ) = self._check_type(column)

            quartile = self._get_quartile(len(col_stats.unique_stats))

            column = column[column != None]

            (
                col_stats.number_stats,
                col_stats.string_stats,
                col_stats.common_stats,
                col_stats.quartile_stats,
            ) = self._calc_statistics(col_stats.column_type, quartile, column)

            column_info = self._make_col_info(col_stats)

            column_info["column_dqi"] = self._calc_col_dqi(
                column_info, column, col_stats, unique_regex, bin_regex, range_info
            )

            self.table_stats["column_stats"].append(column_info)

            # make table dpi
            for key, value in column_info["column_dqi"].items():
                if key not in table_dqi:
                    table_dqi[key] = 0
                table_dqi[key] += value

        column_cnt = len(self.table_stats["column_stats"])
        for key, value in table_dqi.items():
            table_dqi[key] = value / column_cnt

        self.table_stats["table_dqi"] = table_dqi

        return self.table_stats


if __name__ == "__main__":
    file_path = "/Users/cbc/Downloads/corp_num/company_100.csv"

    with open("conf/rule.json", "r") as fd:
        rules = json.load(fd)

    rule = RuleDataQuailty(file_path)
    result = rule.evaluation(rules)

    print(json.dumps(result, indent=3, ensure_ascii=False))
