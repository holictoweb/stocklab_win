import time
from datetime import datetime

import pandas as pd

from apscheduler.schedulers.background import BackgroundScheduler 


from stocklab.agent.ebest import EBest
from stocklab.agent.data import Data
from stocklab.db_handler.mongodb_handler import MongoDBHandler



'''
python -m stocklab.scheduler.data_processing_1d

한화솔루션 009830
'''


#ebest_trade = EBest("PROD")
#ebest_trade.login()
mongo = MongoDBHandler()


def pre_processing(code_list):
    #get mongodb data
    #  mongodb.find_items({"날짜":today}, "stocklab", "price_info").distinct("code")

    today = datetime.today().strftime("%Y%m%d")
    
    price_info = mongo.find_items({"날짜":today}, "stocklab", "price_info")

    collect_list = mongo.find_items({"날짜":today}, "stocklab", "m_price_info").distinct("code")
    for col in collect_list:
        collect_list.remove(col)
        
    
    for code in code_list:
        time.sleep(1)
        print(">>> code collect :", code)
        result_price = ebest_trade.get_price_n_min_by_code(today, today, code, tick=None)
        
        
        df_result_price = pd.DataFrame(result_price)
        df_result_price['code'] = code
        #print(df_result_price)
        df_result_price = df_result_price.to_dict("records")

        #print ( df_result_price)
        
        if len(result_price) > 0:
            #print(result_price)
            mongo.insert_items(df_result_price, "stocklab", "price_prod")
    

    return price_info


def collect_ma():
    
    chart_data = mongo.find_items({}, 'stocklab', 'price_60day_info')
    print(chart_data)
    print("collect_ma")

def collect_data_parquet



if __name__ == "__main__":
    code_list = mongo.find_items({}, "stocklab", "prod_code")
    print(code_list)

    pre_processing(code_list)
