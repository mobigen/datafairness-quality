from pprint import pprint
from DB.IRISDB import IRISDB
import configparser


class RuleException(Exception):
    def __init__(self, message):
        super().__init__(message)


class DQI_RULE:
    def __init__(self, db_info=None, rule_path=None, mode="DB"):
        self.mode = mode  # DB : (IRIS) or FILE : (csv)

        if self.mode == "DB":
            self.db_info = db_info
            self.db = IRISDB(self.db_info)
            self.db.connect_db()
            self.config = self.select_db()
        elif self.mode == "FILE":
            self.rule_path = rule_path
            self.config = configparser.ConfigParser()
            self.config.optionxform = str  # config.ini 구문 분석 시, 대문자->소문자 변환되는 동작 비활성화
            self.config.read(rule_path, encoding="utf-8")

    def write_config(self):
        with open(self.rule_path, "w", encoding="utf-8") as fd:
            self.config.write(fd)

    def split_rule(self, rule):
        regex_rule = []
        regex_set_rule = []
        range_rule = []
        bin_regex_set_rule = []
        unique_regex_set_rule = []

        for key, value in rule.items():
            if key == "regex":
                regex_rule = value
            elif key == "regex_set":
                regex_set_rule = value
            elif key == "range":
                range_rule = value
            elif key == "bin_regex_set":
                bin_regex_set_rule = value
            elif key == "unique_regex_set":
                unique_regex_set_rule = value
            else:
                raise RuleException("unknown rule key : {}".format(key))

        return (
            regex_rule,
            regex_set_rule,
            range_rule,
            bin_regex_set_rule,
            unique_regex_set_rule,
        )

    def select_db(self):
        table_list = ["REGEX", "REGEX_SET", "RANGE", "BIN_SET", "UNIQUE_SET"]
        config = {
            "REGEX": {},
            "REGEX_SET": {},
            "RANGE": {},
            "BIN_SET": {"BIN": {}},
            "UNIQUE_SET": {"UNIQUE": {}},
        }

        for table_name in table_list:
            _, select_data = self.db.select_query(table_name)
            for data in select_data:
                if (
                    table_name == "REGEX"
                    or table_name == "REGEX_SET"
                    or table_name == "RANGE"
                ):
                    config[table_name][data[0]] = data[1]
                elif table_name == "BIN_SET":
                    config[table_name]["BIN"] = data[0]
                elif table_name == "UNIQUE_SET":
                    config[table_name]["UNIQUE"] = data[0]
        return config

    def set_rule(self, set_rule):
        (
            regex_rule,
            regex_set_rule,
            range_rule,
            bin_regex_set_rule,
            unique_regex_set_rule,
        ) = self.split_rule(set_rule)

        # REGEX
        for regex in regex_rule:
            if regex["name"] in self.config["REGEX"]:
                raise RuleException(
                    "[REGEX] already exist REGEX Key : {}".format(regex["name"])
                )
            else:
                # insert
                if self.mode == "DB":
                    self.db.insert_query(
                        "REGEX",
                        ["NAME", "EXPRESSION"],
                        [[regex["name"], regex["expression"]]],
                    )
                self.config["REGEX"][regex["name"]] = regex["expression"]
        # REGEX_SET
        for regex_set in regex_set_rule:
            if regex_set["regex_name"] not in self.config["REGEX"]:
                raise RuleException(
                    "not exist REGEX Key : {}".format(regex_set["regex_name"])
                )
            else:
                if regex_set["name"] in self.config["REGEX_SET"]:
                    regex_list = self.config["REGEX_SET"][regex_set["name"]].split(",")
                    if regex_set["regex_name"] in regex_list:
                        raise RuleException(
                            "[REGEX_SET] already exist REGEX_SET : {}, REGEX Key : {}".format(
                                regex_set["name"], regex_set["regex_name"]
                            )
                        )
                    else:
                        # update
                        update_data = "{},{}".format(
                            self.config["REGEX_SET"][regex_set["name"]],
                            regex_set["regex_name"],
                        )
                        if self.mode == "DB":
                            where_datas = [regex_set["name"]]
                            update_datas = [update_data]
                            self.db.update_query(
                                "REGEX_SET",
                                ["REGEX_NAME"],
                                update_datas,
                                ["NAME"],
                                where_datas,
                            )
                        self.config["REGEX_SET"][regex_set["name"]] = update_data
                else:
                    # insert
                    if self.mode == "DB":
                        self.db.insert_query(
                            "REGEX_SET",
                            ["NAME", "REGEX_NAME"],
                            [[regex_set["name"], regex_set["regex_name"]]],
                        )
                    self.config["REGEX_SET"][regex_set["name"]] = regex_set[
                        "regex_name"
                    ]
        # RANGE
        for range in range_rule:
            if range["name"] in self.config["RANGE"]:
                raise RuleException(
                    "[REGEX] already exist RANGE Key : {}".format(range["name"])
                )
            else:
                # insert
                range_info = "{},{}".format(range["min"], range["max"])
                if self.mode == "DB":
                    self.db.insert_query(
                        "RANGE", ["NAME", "RANGE"], [[range["name"], range_info]]
                    )
                self.config["RANGE"][range["name"]] = range_info
        # BIN_SET
        for bin_regex_set in bin_regex_set_rule:
            bin_list = self.config["BIN_SET"]["BIN"].split(",")
            if bin_regex_set["set_name"] not in self.config["REGEX_SET"]:
                raise RuleException(
                    "[BIN_SET] not exist REGEX_SET Key : {}".format(
                        bin_regex_set["set_name"]
                    )
                )
            else:
                if bin_regex_set["set_name"] in bin_list:
                    raise RuleException(
                        "[BIN_SET] already exist BIN_SET Key : {}".format(
                            bin_regex_set["set_name"]
                        )
                    )
                else:
                    # update
                    where_datas = [",".join(bin_list)]
                    bin_list.append(bin_regex_set["set_name"])
                    update_datas = [",".join(bin_list)]

                    if self.mode == "DB":
                        self.db.update_query(
                            "BIN_SET",
                            ["SET_NAME"],
                            update_datas,
                            ["SET_NAME"],
                            where_datas,
                        )
                    self.config["BIN_SET"]["BIN"] = ",".join(bin_list)
        # UNIQUE_SET
        for unique_regex_set in unique_regex_set_rule:
            unique_list = self.config["UNIQUE_SET"]["UNIQUE"].split(",")
            if unique_regex_set["set_name"] not in self.config["REGEX_SET"]:
                raise RuleException(
                    "[UNIQUE_SET] not exist REGEX_SET Key : {}".format(
                        unique_regex_set["set_name"]
                    )
                )
            else:
                if unique_regex_set["set_name"] in unique_list:
                    raise RuleException(
                        "[UNIQUE_SET] already exist UNIQUE_SET Key : {}".format(
                            unique_regex_set["set_name"]
                        )
                    )
                else:
                    # update
                    where_datas = [",".join(unique_list)]
                    unique_list.append(unique_regex_set["set_name"])
                    update_datas = [",".join(unique_list)]
                    if self.mode == "DB":
                        self.db.update_query(
                            "UNIQUE_SET",
                            ["SET_NAME"],
                            update_datas,
                            ["SET_NAME"],
                            where_datas,
                        )
                    self.config["UNIQUE_SET"]["UNIQUE"] = ",".join(unique_list)

        # Write config
        if self.mode == "FILE":
            self.write_config()

    def delete_rule(self, del_rule):
        (
            regex_rule,
            regex_set_rule,
            range_rule,
            bin_regex_set_rule,
            unique_regex_set_rule,
        ) = self.split_rule(del_rule)

        # BIN_SET
        for bin_regex_set in bin_regex_set_rule:
            bin_list = self.config["BIN_SET"]["BIN"].split(",")
            if bin_regex_set["set_name"] not in bin_list:
                raise RuleException(
                    "[BIN_SET] not exist REGEX_SET Key : {}".format(
                        bin_regex_set["set_name"]
                    )
                )
            else:
                where_datas = [",".join(bin_list)]
                bin_list.remove(bin_regex_set["set_name"])
                update_datas = [",".join(bin_list)]
                if self.mode == "DB":
                    self.db.update_query(
                        "BIN_SET", ["SET_NAME"], update_datas, ["SET_NAME"], where_datas
                    )
                self.config["BIN_SET"]["BIN"] = ",".join(bin_list)
        # UNIQUE_SET
        for unique_regex_set in unique_regex_set_rule:
            unique_list = self.config["UNIQUE_SET"]["UNIQUE"].split(",")
            if unique_regex_set["set_name"] not in unique_list:
                raise RuleException(
                    "[UNIQUE_SET] not exist REGEX_SET Key : {}".format(
                        unique_regex_set["set_name"]
                    )
                )
            else:
                where_datas = [",".join(unique_list)]
                unique_list.remove(unique_regex_set["set_name"])
                update_datas = [",".join(unique_list)]

                if self.mode == "DB":
                    self.db.update_query(
                        "UNIQUE_SET",
                        ["SET_NAME"],
                        update_datas,
                        ["SET_NAME"],
                        where_datas,
                    )
                self.config["UNIQUE_SET"]["UNIQUE"] = ",".join(unique_list)
        # REGEX_SET
        for regex_set in regex_set_rule:
            if regex_set["name"] not in self.config["REGEX_SET"]:
                raise RuleException(
                    "[REGEX_SET] not exist REGEX_SET Key : {}".format(regex_set["name"])
                )
            else:
                bin_list = self.config["BIN_SET"]["BIN"].split(",")
                unique_list = self.config["UNIQUE_SET"]["UNIQUE"].split(",")

                if regex_set["name"] in bin_list or regex_set["name"] in unique_list:
                    raise RuleException(
                        "[REGEX_SET] This is the REGEX_SET Key({}) currently in use. (BIN_SET or UNIQUE_SET)".format(
                            regex_set["name"]
                        )
                    )
                else:
                    regex_list = self.config["REGEX_SET"][regex_set["name"]].split(",")
                    if regex_set["regex_name"] not in regex_list:
                        raise RuleException(
                            "[REGEX_SET] not exist REGEX Key : {}".format(
                                regex_set["regex_name"]
                            )
                        )
                    else:
                        regex_list.remove(regex_set["regex_name"])
                        if len(regex_list) == 0:
                            if self.mode == "DB":
                                self.db.delete_query(
                                    "REGEX_SET", "NAME", regex_set["name"]
                                )
                                del self.config["REGEX_SET"][regex_set["name"]]
                            elif self.mode == "FILE":
                                self.config.remove_option(
                                    "REGEX_SET", regex_set["name"]
                                )
                        else:
                            if self.mode == "DB":
                                where_datas = [regex_set["name"]]
                                update_datas = [",".join(regex_list)]
                                self.db.update_query(
                                    "REGEX_SET",
                                    ["REGEX_NAME"],
                                    update_datas,
                                    ["NAME"],
                                    where_datas,
                                )
                            self.config["REGEX_SET"][regex_set["name"]] = ",".join(
                                regex_list
                            )

        # REGEX
        for regex in regex_rule:
            if regex["name"] not in self.config["REGEX"]:
                raise RuleException(
                    "[REGEX] not exist REGEX Key : {}".format(regex["name"])
                )
            else:
                for regex_set in self.config["REGEX_SET"].keys():
                    if regex["name"] in self.config["REGEX_SET"][regex_set]:
                        raise RuleException(
                            "[REGEX] This is the REGEX Key({}) currently in use. (REGEX_SET)".format(
                                regex["name"]
                            )
                        )
                if self.mode == "DB":
                    self.db.delete_query("REGEX", "NAME", regex["name"])
                    del self.config["REGEX"][regex["name"]]
                elif self.mode == "FILE":
                    self.config.remove_option("REGEX", regex["name"])
        # RANGE
        for range in range_rule:
            if range["name"] not in self.config["RANGE"]:
                raise RuleException(
                    "[RANGE] not exist RANGE Key : {}".format(range["name"])
                )
            else:
                if self.mode == "DB":
                    self.db.delete_query("RANGE", "NAME", range["name"])
                    del self.config["RANGE"][range["name"]]
                elif self.mode == "FILE":
                    self.config.remove_option("RANGE", range["name"])
        # Write config
        if self.mode == "FILE":
            self.write_config()

    def display_rule(self):
        dis_rule = {
            "regex": [],
            "regex_set": [],
            "range": [],
            "bin_regex_set": [],
            "unique_regex_set": [],
        }

        for name, expression in self.config["REGEX"].items():
            tmp = {"name": name, "expression": expression}
            dis_rule["regex"].append(tmp)

        for name, regex_name in self.config["REGEX_SET"].items():
            tmp = {"name": name, "regex_name": regex_name}
            dis_rule["regex_set"].append(tmp)

        for name, range in self.config["RANGE"].items():
            range = range.split(",")
            tmp = {"name": name, "min": range[0], "max": range[1]}
            dis_rule["range"].append(tmp)

        bin_list = self.config["BIN_SET"]["BIN"].split(",")
        for set_name in bin_list:
            tmp = {"set_name": set_name}
            dis_rule["bin_regex_set"].append(tmp)

        unique_list = self.config["UNIQUE_SET"]["UNIQUE"].split(",")
        for set_name in unique_list:
            tmp = {"set_name": set_name}
            dis_rule["unique_regex_set"].append(tmp)

        return dis_rule
