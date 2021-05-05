import time
import inspect
from multiprocessing import Process
from datetime import datetime
import pandas as pd

from apscheduler.schedulers.background import BackgroundScheduler 
from stocklab.agent.ebest import EBest
from stocklab.agent.data import Data


import chardet



# 2021-05-02 별도 DB 연결은 제거 하고 parquet 파일 기반으로 변경 
#from stocklab.db_handler.mongodb_handler import MongoDBHandler
#from stocklab.db_handler.cosmosdb_handler import CosmosDBHandler'


'''
python -m stocklab.scheduler.data_collector_1m
'''

ebest = EBest("PROD")
ebest.login()

#mongodb = MongoDBHandler()


def collect_stock_min(sdate):
    # >> 날짜별로 조회 
    '''
    code_list = mongodb.find_items({}, "stocklab", "code_prod")
    target_code = set([item["shcode"] for item in code_list])
    today = datetime.today().strftime("%Y%m%d")
    # delete data 
    collect_list = mongodb.find_items({"date":today}, "stocklab", "price_min").distinct("code")
    '''
    #spark.read.csv("s3a://mfas-artifact-seoul/glue/tmp/" + file_name2, header = True, inferSchema='false', multiLine=True, quote='"', escape='\\', sep=',',ignoreLeadingWhiteSpace='true',ignoreTrailingWhiteSpace='true',mode='FAILFAST', encoding="UTF-8")
    with open('D:\data\stock_code\stock_code.csv', 'rb') as f:
        result = chardet.detect(f.read())  # or readline if the file is large

    # index는 읽지 않는 것으로 지정 해야함.  혹은 저장 시점에 인덱스 정보 변경 
    pdf = pd.read_csv('D:\data\stock_code\stock_code.csv',  encoding=result['encoding'], index_col=False, compression='gzip')
    today = datetime.today().strftime("%Y%m%d")
    print(pdf)

    '''
    for col in collect_list:
        target_code.remove(col)
    '''
    target_code = pdf['shcode']
    index = 0
    for code in target_code:
        time.sleep(1)
        print(">>> code:", code)
        result_price = ebest.get_price_n_min_by_code(sdate, today, code, tick=None)
        
        df_result_price = pd.DataFrame(result_price)
        df_result_price['shcode'] = code
        #print(df_result_price)
        #dict_result_price = df_result_price.to_dict("records")

        #print ( df_result_price)
        
        if len(result_price) > 0:
            
            # df 와 dict 유형 분리
            df_result_price.to_csv('D:/data/stock_min/'+ code +'.csv', compression='gzip', mode = 'w+')

            #mongodb.insert_items(dict_result_price, "stocklab", "price_min")
            index = index+1
            print(index)
    
if __name__ == '__main__':

    
    print(">>> code list start")
    #collect_code_list()
    
    sdate = '20200101'
    collect_stock_min(sdate)
    
