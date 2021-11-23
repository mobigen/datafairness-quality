from .DataQuality import DataQuality
from .DataQuality import ColumnStats
import operator
import numpy as np


class AutoDataQuality(DataQuality):
    def __init__(self, file_path=None, db_info=None, table_name=None):
        super().__init__(file_path, db_info, table_name)

    def check_pattern(self, column, regex_set, regex_compile):
        pattern_stats = {}
        column_pattern = None
        unknown = 0

        for data in column:
            if data == None:
                continue

            f_match = 0
            for key, value in regex_compile.items():
                result = self.regex_match(key, value, data)
                if result != None:
                    if self.check_valid(key, data) == False:
                        break
                    if key not in pattern_stats:
                        pattern_stats[key] = 0
                    pattern_stats[key] += 1
                    f_match = 1

            if f_match == 0:
                unknown += 1

        if len(pattern_stats) != 0:
            column_pattern = sorted(
                pattern_stats.items(), key=operator.itemgetter(1), reverse=True
            )[0][0]

            for set_name, pattern_list in regex_set.items():
                if column_pattern in pattern_list:
                    column_pattern = set_name
                    break

        return column_pattern, pattern_stats, unknown

    def calc_col_dqi(self, column, col_stats, regex_set, unique_regex, bin_regex):
        data_dqi = {}

        data_dqi["missing_rate"] = self.calc_missing_rate(
            col_stats.missing_count, col_stats.row_count
        )

        max_type_cnt = sorted(
            col_stats.type_stats.items(), key=lambda x: x[1], reverse=True
        )[0][1]

        data_dqi["type_missmatch_rate"] = self.calc_violation_rate(
            max_type_cnt, col_stats.row_count
        )

        if (
            col_stats.missing_count == col_stats.row_count
            or col_stats.column_pattern == None
        ):
            return data_dqi

        max_pattern_cnt = sorted(
            col_stats.pattern_stats.items(), key=lambda x: x[1], reverse=True
        )[0][1]

        regex_set_sum = 0
        for pattern, match_cnt in col_stats.pattern_stats.items():
            if pattern in regex_set[col_stats.column_pattern]:
                regex_set_sum += match_cnt

        data_dqi["pattern_mismatch_rate"] = self.calc_violation_rate(
            max_pattern_cnt, col_stats.row_count
        )

        # 일관성(rule set)을 위배하는 값 측정
        data_dqi["consistency_violation_rate"] = self.calc_violation_rate(
            max_pattern_cnt, regex_set_sum
        )

        data_dqi["outlier_ratio"] = self.calc_outlier_ratio(col_stats, column)

        if col_stats.column_pattern in unique_regex:
            data_dqi["uniqueness_violation_rate"] = self.calc_uniqueness_violation_rate(col_stats)

        if col_stats.column_pattern in bin_regex:
            data_dqi["binary_violation_rate"] = self.calc_violation_rate(
                max_pattern_cnt, regex_set_sum
            )

        return data_dqi

    def evaluation(self, f_ner="off"):
        table_dqi = {}
        (
            regex_compile,
            regex_set,
            unique_regex,
            bin_regex,
            _,
        ) = self.set_rule_for_db()

        for column_name in self._df.columns:
            col_stats = ColumnStats()
            col_stats.column_name = column_name
            col_stats.row_count = len(self._df[column_name])

            print("\n[column name] : {}".format(column_name))

            self._df[column_name] = self._df[column_name].replace(
                r"^\s*$", np.NaN, regex=True
            )

            column = np.where(
                self._df[column_name].isnull(), None, self._df[column_name]
            )

            (
                col_stats.column_pattern,
                col_stats.pattern_stats,
                col_stats.unknown,
            ) = self.check_pattern(column, regex_set, regex_compile)

            (
                col_stats.column_type,
                col_stats.type_stats,
                col_stats.unique_stats,
                col_stats.missing_count,
            ) = self.check_type(column)

            quartile = self.get_quartile(len(col_stats.unique_stats))

            column = column[column != None]

            if len(col_stats.pattern_stats) != 0 and f_ner == "on":
                col_stats.ner = self.get_ner(
                    col_stats, column, column_name, regex_set["TEXT_KOR"]
                )

            (
                col_stats.number_stats,
                col_stats.string_stats,
                col_stats.common_stats,
                col_stats.quartile_stats,
            ) = self.calc_statistics(col_stats.column_type, quartile, column)

            time_distribution = self.get_time_distribution(self._df, column_name)
            if time_distribution != None:
                col_stats.time_distribution = time_distribution

            column_info = self.make_col_info(col_stats)

            column_info["column_dqi"] = self.calc_col_dqi(
                column, col_stats, regex_set, unique_regex, bin_regex
            )

            self.table_stats["column_stats"].append(column_info)

            # make table dpi
            for key, value in column_info["column_dqi"].items():
                if key not in table_dqi:
                    table_dqi[key] = 0
                table_dqi[key] += value
                column_info["column_dqi"][key] = "{:.3f} %".format(value)

        column_cnt = len(self.table_stats["column_stats"])
        for key, value in table_dqi.items():
            table_dqi[key] =  "{:.3f} %".format(value / column_cnt)

        self.table_stats["table_dqi"] = table_dqi

        return self.table_stats
