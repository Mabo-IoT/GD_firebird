import firebirdsql
import datetime
import time

class Firebird:
    def __init__(self):
        self.cursor =  self.connect()
        self.alarm_names = set()

    def connect(self):
        """
        connect to firebird and create a cursor
        """
        conn = firebirdsql.connect(
            host='localhost',
            database='D:\Work\Projects\GD_firebird\EKP.FDB',
            port=3050,
            user='sysdba',
            password='masterkey',
            charset='gbk',
        )

        cur = conn.cursor()
        return cur

    def select_alarm(self):
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

    def select_tank(self, table_name="TANK4"):
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
        date_timestamp = time.mktime(datetime.timetuple())

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

    def run(self):
        
        alarm_data = self.select_alarm()
        alarm_handle_data = self.process_alarm_data(alarm_data)
        print(alarm_handle_data)
        tank_data = self.select_tank()
        tank_handle_data = self.process_tank_data(tank_data)
        print(tank_handle_data)
        # self.select_many()
        # tank_data = select_tank("TANK1")


if __name__ == '__main__':
    fb = Firebird()
    while True:
        fb.run()

