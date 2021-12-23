from .DataQuality import DataQuality
import numpy as np


class Correction(DataQuality):
    def __init__(self, file_path=None, db_info=None, table_name=None):
        super().__init__(file_path, db_info, table_name)

    def correction_pattern(self, column_name, regex_set, regex_compile,
                           column_pattern):
        match_data = {}
        correction_list = []
        for index, data in enumerate(self._df[column_name]):
            if data == None:
                correction_list.append(index)
                continue

            f_match = 0
            for key, value in regex_compile.items():
                if key in regex_set[column_pattern]:
                    result = self.regex_match(key, value, data)
                    if result != None:
                        if data not in match_data:
                            match_data[data] = 0
                        match_data[data] += 1
                        f_match = 1

            if f_match == 0:
                correction_list.append(index)

        mode_data = sorted(match_data.items(),
                           key=lambda x: x[1],
                           reverse=True)[0][0]

        print(correction_list, len(correction_list))
        for index in correction_list:
            self._df[column_name][index] = mode_data

        return len(correction_list)

    def correction_type(self, column_name, column_type):
        correction_list = []
        match_data = {}
        data_type = ""
        for index, data in enumerate(self._df[column_name]):
            if data == None:
                correction_list.append(index)
                continue
            try:
                float(data)
                data_type = "NUMBER"
            except:
                data_type = "STRING"

            if data_type != column_type:
                correction_list.append(index)
            else:
                if data not in match_data:
                    match_data[data] = 0
                match_data[data] += 1

        mode_data = sorted(match_data.items(),
                           key=lambda x: x[1],
                           reverse=True)[0][0]
        print(correction_list, len(correction_list))

        for index in correction_list:
            self._df[column_name][index] = mode_data

        return len(correction_list)

    def correction_missing_value(self, column_name, correction):
        correction_list = []
        match_data = {}

        for index, data in enumerate(self._df[column_name]):
            if data == None:
                correction_list.append(index)
                continue

            if data not in match_data:
                match_data[data] = 0
            match_data[data] += 1

        mode_data = sorted(match_data.items(),
                           key=lambda x: x[1],
                           reverse=True)[0][0]

        print(correction_list, len(correction_list))

        print(correction)
        if correction == "delete":
            print("delete")
            self._df = self._df.drop(index=correction_list, axis=0)
        else:
            print("imputation")
            for index in correction_list:
                self._df[column_name][index] = mode_data
        
        return len(correction_list)

    def make_table_name(self, data_index, column_name):
        table_name = ""
        if data_index == "pattern_missmatch_rate":
            table_name = "PATTERN_{}_{}".format(column_name, self.table_name)
        elif data_index == "type_missmatch_rate":
            table_name = "TYPE_{}_{}".format(column_name, self.table_name)
        elif data_index == "missing_rate":
            table_name = "MISSING_{}_{}".format(column_name, self.table_name)
        else:
            print("unknown data_index ({})".format(data_index))

        return table_name

    def run_correction(self, table_name, column_name, data_index, correction,
                       column_pattern, column_type):
        (
            regex_compile,
            regex_set,
            unique_regex,
            bin_regex,
            range_info,
        ) = self.set_rule_for_db()

        print("\n[column name] : {}".format(column_name))

        table_name = self.make_table_name(data_index, column_name)
        print(table_name)

        self._df[column_name] = self._df[column_name].replace(r"^\s*$",
                                                              np.NaN,
                                                              regex=True)

        self._df[column_name] = np.where(self._df[column_name].isnull(), None,
                                         self._df[column_name])

        correction_cnt = 0

        if data_index == "pattern_missmatch_rate":
            print("column pattern : {}".format(column_pattern))


            correction_cnt = self.correction_pattern(column_name, regex_set, regex_compile,
                                    column_pattern)


        elif data_index == "type_missmatch_rate":
            print("column type : {}".format(column_type))

            correction_cnt = self.correction_type(column_name, column_type)


        elif data_index == "missing_rate":
            correction_cnt = self.correction_missing_value(column_name, correction)

        # drop table
        self.iris.drop_table(table_name)
        # make & insert table
        self.iris.create_table(table_name, self.meta)
        self.iris.bulk_insert_query(table_name, self.meta, self._df)

        return table_name, correction_cnt
