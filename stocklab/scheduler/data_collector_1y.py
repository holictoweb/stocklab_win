import time
import inspect
from multiprocessing import Process
from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler 

from stocklab.agent.ebest import EBest
from stocklab.agent.data import Data


from stocklab.db_handler.mongodb_handler import MongoDBHandler
#from stocklab.db_handler.cosmosdb_handler import CosmosDBHandler




'''
python -m stocklab.scheduler.data_collector_1y
'''
def collect_code_list():
    ebest = EBest("PROD")
    mongodb = MongoDBHandler()
    ebest.login()

    #전체 데이터 수집
    result = ebest.get_code_list("ALL")
    
    mongodb.delete_items({}, "stocklab", "code_info")
    mongodb.insert_items(result, "stocklab", "code_info")

def collect_stock_info():
    ebest = EBest("PROD")
    mongodb = MongoDBHandler()
    ebest.login()
    code_list = mongodb.find_items({}, "stocklab", "code_info")
    
    target_code = set([item["단축코드"] for item in code_list])
    #today = datetime.today().strftime("%Y%m%d")
    today = datetime.today().strftime("%Y%m%d")
    print(today)
    collect_list = mongodb.find_items({"날짜":today}, "stocklab", "price_info") \
                            .distinct("code")


    for col in collect_list:
        target_code.remove(col)
    for code in target_code:
        time.sleep(1)
        print(">>> code:", code)
        result_price = ebest.get_stock_price_by_code(code, "365")
        if len(result_price) > 0:
            #print(result_price)
            mongodb.insert_items(result_price, "stocklab", "price_info")

        result_credit = ebest.get_credit_trend_by_code(code, today)
        if len(result_credit) > 0:
            mongodb.insert_items(result_credit, "stocklab", "credit_info")

        result_short = ebest.get_short_trend_by_code(code, 
                                                    sdate=today, edate=today)
        if len(result_short) > 0:
            mongodb.insert_items(result_short, "stocklab", "short_info")

        '''
        result_agent = ebest.get_agent_trend_by_code(code, 
                                                    fromdt=today, todt=today)
        if len(result_agent) > 0:
            mongodb.insert_items(result_agent, "stocklab", "agent_info")
        '''

    
if __name__ == '__main__':

    
    print(">>> code list start")
    #collect_code_list()
    
    collect_stock_info()
    