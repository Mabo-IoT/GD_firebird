# -*- coding: utf-8 -*-
import time
import firebirdsql
import traceback
from logging import getLogger

from Doctopus.Doctopus_main import Check, Handler


log = getLogger('Doctopus.plugins')


class MyCheck(Check):
    def __init__(self, configuration):
        super(MyCheck, self).__init__(configuration=configuration)
        self.conf = configuration['user_conf']['check']
        self.table_names = self.conf['table_names']
        self.alarm_names = set()
        self.conn =  self.connect(path=self.conf['path'], host=self.conf['host'], port=self.conf['port'], 
                        username=self.conf['username'], passwd=self.conf['passwd'], )
        self.cursor = self.conn.cursor()

    def connect(self, path='../EKP.FDB', host='localhost', port=3050, username='sysdba', passwd='masterkey',):
        """
        connect to firebird and create a cursor
        """
        log.debug(path)
        while True:
            try:
                conn = firebirdsql.connect(
                    host=host,
                    database=path,
                    port=3050,
                    user='sysdba',
                    password='masterkey',
                    charset='gbk',
                )

                if conn:
                    break

            except Exception as e:
                log.error(e)
                log.info("please check if database and trying to connect again!")

        return conn

    def re_connect(self):
        """
        reconnect to database
        """
        self.cursor.close()
        self.conn.close()

        self.conn =  self.connect(path=self.conf['path'], host=self.conf['host'], port=self.conf['port'], 
                        username=self.conf['username'], passwd=self.conf['passwd'], )

        self.cursor = self.conn.cursor()

    def select_tank_row(self, table_name):
        """
        select tank data
        """
        sql = 'select * from {} order by DT desc'
        sql = sql.format(table_name)
        self.cursor.execute(sql)
        row = self.cursor.fetchone()
        # print(row)
        return row

    def process_tank_data(self, tank_data):
        """
        parse tank data to right format which influxdb fields need
        """
        # unpack tank_data
        datetime, M, T, H, P, U1, I1, P1, L1, U2, I2, P2, L2, U3, I3, P3, L3 = tank_data
        # datetime to timestamp
        date_timestamp = int(time.mktime(datetime.timetuple()) * 1000000) # to fit infuxldb timestamp 'us'

        data = {
                "date_timestamp": date_timestamp,
                "M": M,
                "T": T, 
                "H": H,
                "P": P,
                "U1": U1,
                "I1": I1,
                "P1": P1,
                "L1": L1,
                "U2": U2,
                "I2": I2,
                "P2": P2,
                "L2": L2,
                "U3": U3,
                "I3": I3,
                "P3": P3,
                "L3": L3,
                }
        return data

    def add_table_name(self, tank_data, table_name):
        """
        add table_name in fields
        """
        table_dict = {
            "table_name": table_name,
        }
        tank_data.update(table_dict)

        return tank_data

    def user_check(self):
        """

        :param command: user defined parameter.
        :return: the data you requested.
        """
        tank_data_handle = None

        for table_name in self.table_names:
            try:
                # select data from firebire database
                tank_data = self.select_tank_row(table_name)
                # when table have data
                if tank_data:
                # process select data to right fields which influxdb need
                    tank_data_handle = self.process_tank_data(tank_data)
                    tank_data_handle = self.add_table_name(tank_data_handle, table_name)
                else:
                    log.info("{} have no data".format(table_name))
                    tank_data_handle = None
            
            except Exception as e:
                traceback.print_exc()
                log.error(e)
                self.re_connect()

            if tank_data_handle:
                yield tank_data_handle


class MyHandler(Handler):
    def __init__(self, configuration):
        super(MyHandler, self).__init__(configuration=configuration)

    def user_handle(self, raw_data):
        """
        用户须输出一个dict，可以填写一下键值，也可以不填写
        timestamp， 从数据中处理得到的时间戳（整形?）
        tags, 根据数据得到的tag
        data_value 数据拼接形成的 list 或者 dict，如果为 list，则上层框架
         对 list 与 field_name_list 自动组合；如果为 dict，则不处理，认为该数据
         已经指定表名
        measurement 根据数据类型得到的 influxdb表名

        e.g:
        list:
        {'data_value':[list] , required
        'tags':[dict],        optional
        'table_name',[str]   optional
        'timestamp',int}      optional

        dict：
        {'data_value':{'fieldname': value} , required
        'tags':[dict],        optional
        'table_name',[str]   optional
        'timestamp',int}      optional

        :param raw_data: 
        :return: 
        """
        # exmple.
        # 数据经过处理之后生成 value_list
        #log.debug('%s', raw_data)
        
        # extract date_timestamp
        timestamp = raw_data['date_timestamp']
        raw_data.pop('date_timestamp')
        # extract table_name
        table_name = '{0}_{1}'.format(self.table_name, raw_data['table_name'])
        log.debug(table_name)
        raw_data.pop('table_name')

        data_value_list = raw_data

        # user 可以在handle里自己按数据格式制定tags
        user_postprocessed = {'data_value': data_value_list,
                              'timestamp': timestamp,
                              'table_name': table_name,
                                }
        yield user_postprocessed
