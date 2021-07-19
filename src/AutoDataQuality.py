from DataQuality import DataQuality
from DataQuality import ColumnStats
import operator
import numpy as np
import json


class AutoDataQuality(DataQuality):
    def __init__(self, file_path):
        super().__init__(file_path)

    def _check_pattern(self, column, regex_set, regex_compile):
        pattern_stats = {}
        column_pattern = None
        for data in column:
            if data == None:
                continue

            for key, value in regex_compile.items():
                result = self._regex_match(key, value, data)
                if result != None:
                    if self._check_valid(key, data) == False:
                        break
                    if key not in pattern_stats:
                        pattern_stats[key] = 0
                    pattern_stats[key] += 1

        if len(pattern_stats) != 0:
            column_pattern = sorted(
                pattern_stats.items(), key=operator.itemgetter(1), reverse=True
            )[0][0]

            for set_name, pattern_list in regex_set.items():
                if column_pattern in pattern_list:
                    column_pattern = set_name
                    break

        return column_pattern, pattern_stats

    def _calc_col_dqi(
        self, column_info, column, col_stats, regex_set, unique_regex, bin_regex
    ):
        data_dqi = {}

        if (
            col_stats.missing_cnt == col_stats.row_count
            or col_stats.column_pattern == None
        ):
            return data_dqi

        max_pattern_cnt = sorted(
            col_stats.pattern_stats.items(), key=lambda x: x[1], reverse=True
        )[0][1]

        max_type_cnt = sorted(
            col_stats.type_stats.items(), key=lambda x: x[1], reverse=True
        )[0][1]

        data_dqi["missing_rate"] = self._calc_missing_rate(
            column_info["missing_count"], column_info["row_count"]
        )

        data_dqi["type_missmatch_rate"] = self._calc_violation_rate(
            max_type_cnt, column_info["row_count"]
        )

        sum_match_cnt = 0
        column_pattern = column_info["column_pattern"]
        for pattern, match_cnt in col_stats.pattern_stats.items():
            if pattern in regex_set[column_pattern]:
                sum_match_cnt += match_cnt

        data_dqi["pattern_mismatch_rate"] = self._calc_violation_rate(
            sum_match_cnt, column_info["row_count"]
        )

        data_dqi["consistency_violation_rate"] = self._calc_violation_rate(
            max_pattern_cnt, sum_match_cnt
        )

        data_dqi["outlier_ratio"] = self._calc_outlier_ratio(column_info, column)

        if column_info["column_pattern"] in unique_regex:
            data_dqi[
                "uniqueness_violation_rate"
            ] = self._calc_uniqueness_violation_rate(
                column_info["row_count"], col_stats
            )

        if column_info["column_pattern"] in bin_regex:
            data_dqi["binary_violation_rate"] = self._calc_violation_rate(
                max_pattern_cnt, column_info["row_count"]
            )

        return data_dqi

    def evaluation(self):
        table_dqi = {}
        regex_compile, regex_set, unique_regex, bin_regex, _ = self._set_regex()

        for column_name in self._df.columns:
            col_stats = ColumnStats()
            col_stats.column_name = column_name
            col_stats.row_count = len(self._df[column_name])

            column = np.where(
                self._df[column_name].isnull(), None, self._df[column_name]
            )

            col_stats.column_pattern, col_stats.pattern_stats = self._check_pattern(
                column, regex_set, regex_compile
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
                column_info, column, col_stats, regex_set, unique_regex, bin_regex
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
    file_path = "/Users/cbc/Downloads/corp_num/company_1000.csv"

    auto = AutoDataQuality(file_path)
    result = auto.evaluation()

    print(json.dumps(result, indent=3, ensure_ascii=False))
