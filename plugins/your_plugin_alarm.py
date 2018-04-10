# -*- coding: utf-8 -*-
import time
from logging import getLogger

from Doctopus.Doctopus_main import Check, Handler

log = getLogger('Doctopus.plugins')


class MyCheck(Check):
    def __init__(self, configuration):
        super(MyCheck, self).__init__(configuration=configuration)
        self.conf = configuration['user_conf']['check']
        self.alarm_names = set()
        self.cursor =  self.connect(self.conf['host'], self.conf['port'], self.conf['username'], self.conf['passwd'])

    def connect(self, host='localhost', port=3050, username='sysdba', passwd='masterkey'):
        pass

    def select_alarm_row(self):
        """
        select alarm data
        """
        sql = 'select * from HISTALARM order by DT desc'
        self.cursor.execute(sql)
        row = self.cursor.fetchone()
        # print(row)
        return row

    def handle_warning_state(self, alarm_name, state):
        """
        maintain alarm_names and warning state and warning string
        """
        # maintain alarm_name list
        if state == '报警发生':
            self.alarm_names.add(alarm_name)
        else:
            # maybe self.alarm_names is empty
            if alarm_name in self.alarm_names:
                self.alarm_names.remove(alarm_name)
            else:
                pass
        # change warning state
        if self.alarm_names:
            warning = 1
        else:
            warning = 0

        warning_string = ';'.join(self.alarm_names)

        return warning, warning_string

    def process_alarm_data(self, alarm_data):
        """
        parse alarm data to right format which influxdb fields need
        """
        alarm_id, alarm_name, datetime, state, remark = alarm_data
        # datetime to timestamp
        date_timestamp = time.mktime(datetime.timetuple())

        warning, warning_string = self.handle_warning_state(alarm_name, state)

        data = {
                "warning": warning,
                "warning_string": warning_string, 
                "alarm_id": alarm_id,
                "alarm_name": alarm_name,
                "date_timestamp": date_timestamp,
                "state": state,
                "remark": remark,
                }
        return data

    def user_check(self):
        """

        :param command: user defined parameter.
        :return: the data you requested.
        """
        # select data from firebire database
        alarm_data = self.select_alarm_row()
        # process select data to right fields which influxdb need
        alarm_data_handle = self.process_alarm_data(alarm_data)
        
        data = 'check的data'
        log.debug('%s', data)
        time.sleep(2)
        yield data


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
        log.debug('%s', raw_data)
        data_value_list = [raw_data]

        tags = {'user_defined_tag': 'data_ralated_tag'}

        # user 可以在handle里自己按数据格式制定tags
        user_postprocessed = {'data_value': data_value_list,
                              'tags': tags, }
        yield user_postprocessed
