import unittest
import sys
print(sys.path)
from fdb_connect import Firebird


class TestFirebirdFunc(unittest.TestCase):

    def setUp(self):
        self.fb = Firebird()
        print("i am here")

    def test_select_alarm(self):
        self.assertIsNotNone(self.fb.select_alarm())

    def test_select_tank(self):
        self.assertIsNotNone(self.fb.select_tank())
    
    def test_process_alarm_data(self):
        row = self.fb.select_alarm()
        data = self.fb.process_alarm_data(row)
        self.assertIsInstance(data, dict)

    def test_handle_warning_state(self):
        
        self.fb.alarm_names = set()
        test_alarm_name = '测试报警'
        test_state = '报警发生'
        
        warning, warning_string = self.fb.handle_warning_state(test_alarm_name, test_state) 
        self.assertEqual(warning, 1)
        self.assertEqual(warning_string, '测试报警')

    def test_process_tank_data(self):
        row = self.fb.select_tank()
        data = self.fb.process_tank_data(row)
        self.assertIsInstance(data, dict)


if __name__ == '__main__':
    suite = unittest.TestSuite()
    tests = [TestFirebirdFunc("test_select_alarm"), TestFirebirdFunc("test_select_tank"), 
             TestFirebirdFunc("test_handle_warning_state"), TestFirebirdFunc("test_process_alarm_data"),
             TestFirebirdFunc("test_process_tank_data"),]
    suite.addTests(tests)
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)


    

    