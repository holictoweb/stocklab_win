import unittest
from stocklab.agent.ebest import EBest
import inspect
import time

'''
# [T-01] 테스트 케이스 
python -m unittest tests.test_agent_ebest

'''


class TestEBest (unittest.TestCase):

    def setUp(self):
        self.ebest = EBest("PROD")
        self.ebest.login()


    '''
    def test_get_current_call_price_by_code(self):
        print(inspect.stack()[0][3])
        result = self.ebest.get_current_call_price_by_code("005930")
        assert result
        print(result)
    '''

    '''
    def test_get_price_n_min_by_code(self):
        print(inspect.stack()[0][3])
        result = self.ebest.get_price_n_min_by_code("20200801","20200823", "180640")
        assert result
        print(result)
    



    def test_get_price_n_min_by_code_tick(self):
        print(inspect.stack()[0][3])
        result = self.ebest.get_price_n_min_by_code("20200801","20200823", "005930", 0)
        assert result
        print(result)
     

    
    
    def test_get_code_list(self):
        print('>>>  ' + inspect.stack()[0][3])
        all_result = self.ebest.get_code_list("ALL")
        assert all_result is not None
        kosdaq_result = self.ebest.get_code_list("KOSDAQ")
        assert kosdaq_result is not None
        kospi_result = self.ebest.get_code_list("KOSPI")
        assert kospi_result is not None
        
        try:
            error_result = self.ebest.get_code_list("KOS")
        except:
            error_result = None
        
        assert error_result is None
        print("result:", len(all_result), len(kosdaq_result), len(kospi_result))
    

    def test_get_stock_price_by_code(self):
        print('>>>  ' + inspect.stack()[0][3])
        result = self.ebest.get_stock_price_by_code("005930", "2")
        assert result is not None
        print(result)


    
    
    def test_get_account_info(self):
        result = self.ebest.get_account_info()
        assert result is not None
        print(result)


    
    def test_get_account_stock_info(self):
        result = self.ebest.get_account_stock_info()
        assert result is not None
        print(result)    
    

    

    def test_order_stock(self):
        print('>>>  ' + inspect.stack()[0][3])
        result = self.ebest.order_stock("005930", "2", "50000", "2", "00")
        assert result is not None
        print (result)


    def test_order_cancel(self):
        print('>>>  ' + inspect.stack()[0][3])
        result = self.ebest.order_cancel("29515", "A005930", "2")
        assert result
        print(result)

    def test_order_check(self):
        print('>>>  ' + inspect.stack()[0][3])
        result = self.ebest.order_check("29515")
        assert result
        print(result)
    
    '''

    
    def test_get_price_n_min_by_code(self):
        print(inspect.stack()[0][3])
        result = self.ebest.get_price_n_min_by_code("20200801","20200823", "180640")
        assert result
        print(result)

    def tearDown(self):
        self.ebest.logout()