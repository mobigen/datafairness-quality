import datetime as dt
import DB.M6 as M6

class IRISDB():
    def __init__(self, iris_info):
        self.iris_info = iris_info

    def connect_db(self):
        try:
            self.conn = M6.Connection(
                addr_info=self.iris_info['ADDR'],
                id=self.iris_info['USER_ID'],
                password=self.iris_info['PASSWD'],
                Database=self.iris_info['DB_NAME'])
            assert isinstance(self.conn, M6.M6_Connection.Connection)
            self.cur = self.conn.cursor()
        except Exception as e:
            print(e)

    def insert_query(self, table_name, table_fields, insert_data):
        try:
            for data in insert_data:
                sql = 'INSERT INTO {tbl_name} ({tbl_fields}) VALUES'.format(
                    tbl_name=table_name,
                    tbl_fields=', '.join(table_fields)
                    )

                sql_values = ', '.join(list(map(lambda v: '\''+str(v)+'\'', data)))

                sql = sql + f' ( {sql_values} );'
                print(sql)
                res = self.cur.Execute2(sql)
                print(res)
        except Exception as e:
            print(e)

    def delete_query(self, table_name, table_field, delete_data):
        try:
            sql = "DELETE FROM {} WHERE {}=\'{}\';".format(table_name, table_field, delete_data)
            print(sql)
            res = self.cur.Execute2(sql)
            print(res)
        except Exception as e:
            print(e)

    def update_query(self, table_name, update_fields, update_datas, where_fields, where_datas):
        try:
            update_info = []
            for index, field in enumerate(update_fields):
                tmp = "{}=\'{}\'".format(field, update_datas[index])
                update_info.append(tmp)
            
            where_info = []
            for index, field in enumerate(where_fields):
                tmp = "{}=\'{}\'".format(field, where_datas[index])
                where_info.append(tmp)

            sql = "UPDATE {} SET {} WHERE {};".format(table_name, ",".join(update_info), ",".join(where_info))
            print(sql)
            res = self.cur.Execute2(sql)
            print(res)        
        except Exception as e:
            print(e)

    def select_query(self, table_name):
        select_data = None
        meta = None
        print(table_name)
        try:
            res = self.cur.Execute2("SELECT * FROM {};".format(table_name))
            print(res)
            self.cur.ReadData()
            select_data = self.cur.buffer
            meta = self.cur.Metadata()
        except Exception as e:
            print(e)

        return meta["ColumnName"], select_data

    def __del__(self):
        self.cur.Close()
        self.conn.close()    
