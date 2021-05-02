# -*- coding: utf-8 -*-

import time
import inspect
from multiprocessing import Process
from datetime import datetime
#import pandas as pd

from apscheduler.schedulers.background import BackgroundScheduler 
from stocklab.agent.ebest import EBest
from stocklab.agent.data import Data
from stocklab.db_handler.mongodb_handler import MongoDBHandler


'''
python -m stocklab.scheduler.data_collector_1d

'''


ebest = EBest("PROD")
mongodb = MongoDBHandler()
ebest.login()


def run_process_collect_stock_info():
    print(inspect.stack()[0][3])
    p = Process(target=collect_stock_info)
    p.start()
    p.join()


def collect_stock_day(day_count):
    
    code_list = mongodb.find_items( {}, "stocklab", "code_prod")
    target_code = set([item["shcode"] for item in code_list])

    # delete data 
    mongodb.delete_items({}, "stocklab", "price_day")
    
    index = 0
    for code in target_code:
        time.sleep(1)
        #print(">>>> collect price info - code:", code)
        result_price = ebest.get_stock_price_by_code(code, day_count )

        if len(result_price) > 0:
            #print(">>>> collected price info : ", code )
            #print( result_price )
            mongodb.insert_items(result_price, "stocklab", "price_day")
            #print(">>>> collected price into mondgodb : ", code )
            index = index+1
            print(index)
        #print(">>>> finish collect : ", code)
    
    #print(">>>> [collect_stock_d_120] finished.... ")

def collect_stock_info():
    ebest = EBest("PROD")
    mongodb = MongoDBHandler()
    ebest.login()
    
    code_list = mongodb.find_items( {}, "stocklab", "code_prod")
    target_code = set([item["shcode"] for item in code_list])

    today = datetime.today().strftime("%Y%m%d")
    print(today)

    
    collect_list = mongodb.find_items({"날짜":today}, "stocklab", "price_info") \
                            .distinct("code")
    for col in collect_list:
        target_code.remove(col)


    for code in target_code:
        time.sleep(1)
        print("code:", code)
        result_price = ebest.get_stock_price_by_code(code, "1")


        if len(result_price) > 0:
            print(result_price)
            mongodb.insert_items(result_price, "stocklab", "price_info")

        result_credit = ebest.get_credit_trend_by_code(code, today)
        if len(result_credit) > 0:
            mongodb.insert_items(result_credit, "stocklab", "credit_info")

        result_short = ebest.get_short_trend_by_code(code, 
                                                    sdate=today, edate=today)
        if len(result_short) > 0:
            mongodb.insert_items(result_short, "stocklab", "short_info")

        result_agent = ebest.get_agent_trend_by_code(code, 
                                                    fromdt=today, todt=today)
        if len(result_agent) > 0:
            mongodb.insert_items(result_agent, "stocklab", "agent_info")
    




    
if __name__ == '__main__':
    #print(">>> collect_stock_d_120....")
    collect_stock_day(1095)

    #단일 배치용
    #collect_stock_info()

    #스케쥴러 사용
    '''
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=run_process_collect_code_list, trigger="cron", 
                    day_of_week="mon-fri", hour="19", minute="00", id="1")
    scheduler.add_job(func=run_process_collect_stock_info, trigger="cron", 
                    day_of_week="mon-fri", hour="19", minute="05", id="2")
    scheduler.start()
    
    
    while True:
        print("running", datetime.now())
        time.sleep(1)
    '''
    