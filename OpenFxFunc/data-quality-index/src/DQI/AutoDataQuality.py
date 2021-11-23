from textwrap import indent
from .DataQuality import DataQuality
from .DataQuality import ColumnStats
import operator
import numpy as np
from pprint import pprint


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
            column_pattern = sorted(pattern_stats.items(),
                                    key=operator.itemgetter(1),
                                    reverse=True)[0][0]

            for set_name, pattern_list in regex_set.items():
                if column_pattern in pattern_list:
                    column_pattern = set_name
                    break

        return column_pattern, pattern_stats, unknown

    def calc_col_dqi(self, col_stats, regex_set, unique_regex):
        data_dqi = {}

        data_dqi["completeness"] = 100 - self.calc_missing_rate(
            col_stats.missing_count, col_stats.row_count)

        if (col_stats.missing_count == col_stats.row_count
                or col_stats.column_pattern == None):
            return data_dqi

        max_pattern_cnt = sorted(col_stats.pattern_stats.items(),
                                 key=lambda x: x[1],
                                 reverse=True)[0][1]

        regex_set_sum = 0
        for pattern, match_cnt in col_stats.pattern_stats.items():
            if pattern in regex_set[col_stats.column_pattern]:
                regex_set_sum += match_cnt

        data_dqi["consistency"] = 100 - self.calc_violation_rate(
            max_pattern_cnt, col_stats.row_count)

        if col_stats.column_pattern in unique_regex:
            data_dqi["uniqueness"] = 100 - self.calc_uniqueness_violation_rate(
                col_stats)

        return data_dqi

    def evaluation(self):
        table_dqi = {}
        (
            regex_compile,
            regex_set,
            unique_regex,
            _,
            _,
        ) = self.set_rule_for_db()

        for column_name in self._df.columns:
            col_stats = ColumnStats()
            col_stats.column_name = column_name
            col_stats.row_count = len(self._df[column_name])

            print("\n[column name] : {}".format(column_name))

            self._df[column_name] = self._df[column_name].replace(r"^\s*$",
                                                                  np.NaN,
                                                                  regex=True)

            column = np.where(self._df[column_name].isnull(), None,
                              self._df[column_name])

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

            (
                col_stats.number_stats,
                col_stats.string_stats,
                col_stats.common_stats,
                col_stats.quartile_stats,
            ) = self.calc_statistics(col_stats.column_type, quartile, column)

            time_distribution = self.get_time_distribution(
                self._df, column_name)
            if time_distribution != None:
                col_stats.time_distribution = time_distribution

            column_info = self.make_col_info(col_stats)

            column_info["column_dqi"] = self.calc_col_dqi(
                col_stats, regex_set, unique_regex)

            self.table_stats["column_stats"].append(column_info)

            # make table dpi
            for key, value in column_info["column_dqi"].items():
                if key not in table_dqi:
                    table_dqi[key] = 0
                table_dqi[key] += value
                column_info["column_dqi"][key] = "{:.3f} %".format(value)

        column_cnt = len(self.table_stats["column_stats"])
        for key, value in table_dqi.items():
            table_dqi[key] = "{:.3f} %".format(value / column_cnt)

        return self.table_stats

    def check_pattern_for_official_test(self, column, regex_set,
                                        regex_compile):
        pattern_stats = {}
        column_pattern = None
        column_pattern_index = {}
        unknown = 0

        for index, data in enumerate(column):
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

                    if key not in column_pattern_index:
                        column_pattern_index[key] = []
                    column_pattern_index[key].append(index)

                    f_match = 1

            if f_match == 0:
                unknown += 1

        mod_pattern_name = None
        if len(pattern_stats) != 0:
            column_pattern = sorted(pattern_stats.items(),
                                    key=operator.itemgetter(1),
                                    reverse=True)[0][0]

            mod_pattern_name = column_pattern

            for set_name, pattern_list in regex_set.items():
                if column_pattern in pattern_list:
                    column_pattern = set_name
                    break

        return (
            column_pattern,
            pattern_stats,
            unknown,
            column_pattern_index[mod_pattern_name],
        )

    def data_correction(self, column_name, column_pattern_index,
                        duplicate_list, dqi, correction):
        delete_row_index = []
        if dqi == "consistency":
            for index, _ in self._df.iterrows():
                if index not in column_pattern_index:
                    delete_row_index.append(index)
            if correction == "row_delete":
                self._df = self._df.drop(index=delete_row_index, axis=0)
            else:
                print("Unknown correction [ dqi : consistency ]")
        elif dqi == "completeness":
            if correction == "row_delete":
                self._df[column_name] = self._df[column_name].replace(
                    r"^\s*$", np.NaN, regex=True)
                column = np.where(self._df[column_name].isnull(), None,
                                  self._df[column_name])
                for index, data in enumerate(column):
                    if data == None:
                        delete_row_index.append(index)
                self._df = self._df.drop(index=delete_row_index, axis=0)
            else:
                print("Unknown correction [ dqi : completeness ]")
        elif dqi == "uniqueness":
            if correction == "row_delete":
                for index, data in enumerate(self._df[column_name]):
                    if data in duplicate_list:
                        delete_row_index.append(index)
                self._df = self._df.drop(index=delete_row_index, axis=0)
            else:
                print("Unknown correction [ dqi : uniqueness ]")

    def cal_col_dqi_for_official_test(self, col_stats, unique_regex, dqi):
        data_dqi = {}
        if (col_stats.missing_count == col_stats.row_count
                or col_stats.column_pattern == None):
            return data_dqi

        if dqi == "completeness":
            data_dqi[dqi] = "{:.3f} %".format(100 - self.calc_missing_rate(
                col_stats.missing_count, col_stats.row_count))
        elif dqi == "consistency":
            max_pattern_cnt = sorted(col_stats.pattern_stats.items(),
                                     key=lambda x: x[1],
                                     reverse=True)[0][1]

            data_dqi[dqi] = "{:.3f} %".format(
                100 -
                self.calc_violation_rate(max_pattern_cnt, col_stats.row_count))
        elif dqi == "uniqueness":
            if col_stats.column_pattern in unique_regex:
                data_dqi[dqi] = "{:.3f} %".format(
                    100 - self.calc_uniqueness_violation_rate(col_stats))

        return data_dqi

    def dqi_eval(self, column_name, regex_compile, regex_set, unique_regex,
                 step, dqi):
        self._df[column_name] = self._df[column_name].replace(r"^\s*$",
                                                              np.NaN,
                                                              regex=True)
        column = np.where(self._df[column_name].isnull(), None,
                          self._df[column_name])

        col_stats = ColumnStats()
        col_stats.column_name = column_name
        col_stats.row_count = len(column)

        (
            col_stats.column_pattern,
            col_stats.pattern_stats,
            col_stats.unknown,
            column_pattern_index,
        ) = self.check_pattern_for_official_test(column, regex_set,
                                                 regex_compile)

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

        time_distribution = self.get_time_distribution(self._df, column_name)
        if time_distribution != None:
            col_stats.time_distribution = time_distribution

        column_info = self.make_col_info(col_stats)

        column_info["column_dqi"] = self.cal_col_dqi_for_official_test(
            col_stats, unique_regex, dqi)

        self.correction_stat[step] = column_info

        return col_stats, column_pattern_index

    def data_quality_correction(self, column_name, dqi, correction):
        (
            regex_compile,
            regex_set,
            unique_regex,
            _,
            _,
        ) = self.set_rule_for_db()

        col_stats, column_pattern_index = self.dqi_eval(
            column_name, regex_compile, regex_set, unique_regex, "before", dqi)

        duplicate_list = []
        for key, value in col_stats.unique_stats.items():
            if value > 1:
                duplicate_list.append(key)

        self.data_correction(column_name, column_pattern_index, duplicate_list,
                             dqi, correction)

        col_stats, _ = self.dqi_eval(column_name, regex_compile, regex_set,
                                     unique_regex, "after", dqi)
        return self.correction_stat
