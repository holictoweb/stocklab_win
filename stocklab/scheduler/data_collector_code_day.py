# -*- coding: utf-8 -*-

import time
import inspect
from multiprocessing import Process
from datetime import datetime
import pandas as pd

#from apscheduler.schedulers.background import BackgroundScheduler 

from stocklab.agent.ebest import EBest
from stocklab.agent.data import Data
#from stocklab.db_handler.mongodb_handler import MongoDBHandler


'''
python -m stocklab.scheduler.data_collector_code_day

'''

ebest = EBest("PROD")
ebest.login()

#mongodb = MongoDBHandler()

def collect_code_list():
    
    '''
    #전체 데이터 수집
    result = ebest.get_code_list("ALL")

    mongodb.delete_items({}, "stocklab", "code_info")
    mongodb.insert_items(result, "stocklab", "code_info")

    code_list = mongodb.find_items( {"jnilclose":{"$gt":2000,"$lt":30000} } , "stocklab", "code_info")
    '''
    result_code = ebest.get_code_list("ALL")
    #print ( result_code )
    pdf = pd.DataFrame(result_code)
    print ( pdf)
    df_result_code = pdf[ ( pdf.jnilclose> '3000') | ( pdf.jnilclose < '100000')]
    print ( df_result_code)
    df_result_code.to_csv('D:/data/stock_code/stock_code.csv', compression ='gzip', mode = 'w+')
    #df_result_code.to_parquet('D:\\data\\stock_code\\', compression='GZIP')
    
    # pymongo 형 변환 필요 
    #db.code_info.find().forEach( function (d) {     d.jnilclose= parseInt(d.jnilclose);     db.code_info.save(d); });

    print(">>>> end code collect....")


if __name__ == '__main__':
    #코드 리스트 수집
    print(">>> collect_codelist....")
    collect_code_list()

    print(">>> select target code... ")