import time
import inspect
from multiprocessing import Process
from datetime import datetime
import pandas as pd

from apscheduler.schedulers.background import BackgroundScheduler 
from stocklab.agent.ebest import EBest
from stocklab.agent.data import Data
from stocklab.db_handler.mongodb_handler import MongoDBHandler
#from stocklab.db_handler.cosmosdb_handler import CosmosDBHandler'



'''
python -m stocklab.scheduler.data_collector_1m
'''

print("start collect data")
ebest = EBest("PROD")
mongodb = MongoDBHandler()
ebest.login()


def collect_stock_min():
    # >> 날짜별로 조회 
    

    code_list = mongodb.find_items({}, "stocklab", "code_prod")
    target_code = set([item["shcode"] for item in code_list])
    
    today = datetime.today().strftime("%Y%m%d")

    # delete data 
    collect_list = mongodb.find_items({"date":today}, "stocklab", "price_min").distinct("code")
    for col in collect_list:
        target_code.remove(col)
    
    index = 0
    for code in target_code:
        time.sleep(1)
        #print(">>> code:", code)
        result_price = ebest.get_price_n_min_by_code("20200101", today, code, tick=None)
        
        df_result_price = pd.DataFrame(result_price)
        df_result_price['shcode'] = code
        #print(df_result_price)
        dict_result_price = df_result_price.to_dict("records")

        #print ( df_result_price)
        
        if len(result_price) > 0:
            
            # df 와 dict 유형 분리
            df_result_price.to_parquet('c:\data', compression='GZIP')

            mongodb.insert_items(dict_result_price, "stocklab", "price_min")
            index = index+1
            print(index)
    
if __name__ == '__main__':

    
    print(">>> code list start")
    #collect_code_list()
    

    collect_stock_min()
    