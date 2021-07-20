from DataQuality import DataQuality
from DataQuality import ColumnStats
import numpy as np
import json


class RuleDataQuailty(DataQuality):
    def __init__(self, file_path):
        super().__init__(file_path)

    def set_rule(self, regex_set, rules):
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

    def check_range(self, column, pattern_name, range_info):
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

    def check_pattern(self, column, column_name, column_rule, regex_set, regex_compile):
        pattern_stats = {}
        set_name = column_rule[column_name]

        if set_name == "STATS":
            return pattern_stats

        for pattern_name in regex_set[set_name]:
            pattern_stats[pattern_name] = 0

        for data in column:
            if data == None:
                continue

            for pattern_name in regex_set[set_name]:
                result = self.regex_match(
                    pattern_name, regex_compile[pattern_name], data
                )
                if result != None:
                    if self.check_valid(pattern_name, data) == False:
                        continue
                    pattern_stats[pattern_name] += 1

        return pattern_stats

    def calc_col_dqi(self, column, col_stats, unique_regex, bin_regex, range_info):
        data_dqi = {}

        if (
            col_stats.missing_count == col_stats.row_count
            or col_stats.column_pattern == "STATS"
        ):
            return data_dqi

        max_pattern_cnt = sorted(
            col_stats.pattern_stats.items(), key=lambda x: x[1], reverse=True
        )[0][1]

        max_type_cnt = sorted(
            col_stats.type_stats.items(), key=lambda x: x[1], reverse=True
        )[0][1]

        data_dqi["missing_rate"] = self.calc_missing_rate(
            col_stats.missing_count, col_stats.row_count
        )

        data_dqi["type_missmatch_rate"] = self.calc_violation_rate(
            max_type_cnt, col_stats.row_count
        )

        sum_match_cnt = 0
        for match_cnt in col_stats.pattern_stats.values():
            sum_match_cnt += match_cnt

        data_dqi["pattern_mismatch_rate"] = self.calc_violation_rate(
            sum_match_cnt, col_stats.row_count
        )

        data_dqi["consistency_violation_rate"] = self.calc_violation_rate(
            max_pattern_cnt, sum_match_cnt
        )

        data_dqi["outlier_ratio"] = self.calc_outlier_ratio(col_stats, column)

        if col_stats.column_pattern in unique_regex:
            data_dqi["uniqueness_violation_rate"] = self.calc_uniqueness_violation_rate(
                col_stats.row_count, col_stats.unique_stats
            )

        if col_stats.column_pattern in range_info:
            data_dqi["range_violation_rate"] = self.calc_violation_rate(
                col_stats.pattern_stats["MATCH"], col_stats.row_count
            )

        if col_stats.column_pattern in bin_regex:
            data_dqi["binary_violation_rate"] = self.calc_violation_rate(
                max_pattern_cnt, col_stats.row_count
            )

        return data_dqi

    def evaluation(self, rules):
        table_dqi = {}

        (
            regex_compile,
            regex_set,
            unique_regex,
            bin_regex,
            range_info,
        ) = self.set_regex()

        column_rule = self.set_rule(regex_set, rules)
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
                col_stats.pattern_stats = self.check_range(
                    column, col_stats.column_pattern, range_info
                )
            else:
                col_stats.pattern_stats = self.check_pattern(
                    column, column_name, column_rule, regex_set, regex_compile
                )

            (
                col_stats.column_type,
                col_stats.type_stats,
                col_stats.unique_stats,
                col_stats.missing_count,
            ) = self.check_type(column)

            quartile = self.get_quartile(len(col_stats.unique_stats))

            column = column[column != None]

            (
                col_stats.number_stats,
                col_stats.string_stats,
                col_stats.common_stats,
                col_stats.quartile_stats,
            ) = self.calc_statistics(col_stats.column_type, quartile, column)

            column_info = self.make_col_info(col_stats)

            column_info["column_dqi"] = self.calc_col_dqi(
                column, col_stats, unique_regex, bin_regex, range_info
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
    file_path = "sample_data/company_100.csv"

    with open("conf/rule.json", "r") as fd:
        rules = json.load(fd)

    rule = RuleDataQuailty(file_path)
    result = rule.evaluation(rules)

    print(json.dumps(result, indent=3, ensure_ascii=False))
