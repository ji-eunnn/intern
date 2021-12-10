import os
import configparser

import pymysql
import pandas as pd


class DataBaseHandler():
    def __init__(self, db_type='mysql', path_config='./config/config.ini'):
        self.db_type = db_type
        self.path_config = path_config

        if os.path.exists(self.path_config):
            print('[INFO] Exist config file')
            self.config = configparser.ConfigParser()
            self.config.read(self.path_config)
        else:
            print

    def set_mysql(self, name):
        name = name.upper()
        print(name)
        if self.config.has_section(name):
            print('[INFO] open {} section'.format(name))
            self.set_mysql_from_config(self.config[name])
        else:
            print('[INFO] Can not find {} section'.format(name))

    # mysql
    def set_mysql_from_config(self, config):
        self.dict_info = {
            'host': config.get('HOST'),
            'user': config.get('USER'),
            'pw': config.get('PW'),
            'port': int(config.get('PORT')),
            'db': config.get('DB'),
            'charset': config.get('CHARSET')
        }

        self.set_mysql_from_dict(self.dict_info)

    def set_mysql_from_dict(self, dict_info):
        self.dict_info = dict_info

        self.db_mysql = pymysql.connect(host=dict_info.get('host'),
                             user=dict_info.get('user'),
                             passwd=dict_info.get('pw'),
                             port=dict_info.get('port'),
                             db=dict_info.get('db'),
                             charset=dict_info.get('charset'))

        self.db_mysql.autocommit(True)
        self.cursor_mysql = self.db_mysql.cursor(pymysql.cursors.DictCursor)

    def execute_mysql(self, *sql, **kwargs):
        try:
            self.cursor_mysql.execute(*sql)
        except Exception as e:
            print(e)
            self.set_mysql_from_dict(self.dict_info)

        if 'select' in sql[0].lower():
            if kwargs.get('one'):
                result = self.cursor_mysql.fetchone()
            else:
                result = self.cursor_mysql.fetchall()

            return result
        else:
            return None

    def execute_mysql_dataframe(self, sql):
        df_tmp = pd.read_sql_query(sql, self.db_mysql)
        return df_tmp


if __name__ == '__main__':
    dbh = DataBaseHandler(path_config='./config.ini')

    dbh.set_mysql("analysis")

    sql = 'select * from a_imdb_basics_movie_review limit 10'
    row = dbh.execute_mysql(sql)
    print(row[0])