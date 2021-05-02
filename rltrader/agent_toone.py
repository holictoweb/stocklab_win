


import numpy as np


class Agent:

    # 매매 수수료
    TRADING_CHARGE = 0.015
    TRADING_TAX = 0.3

    # Action
    ACTION_BUY = 0 
    ACTION_SELL = 1 
    ACTION_HOLD = 2 
    ACTIONS = [ ACTION_BUY, ACTION_HOLD, ACTION_SELL]


    def __init__(self, environment, min_trading_unit = 1, max_trading_unit = 10 ):

        self.environment = envirionment 


        self.min_trading_unit = min_trading_unit
        self.max_trading_unit = max_trading_unit


        # agnet 클래스 속성
        self.initial_balance = 0
        self.balance = 0
        self.num_stocks = 0 
        self.pv = 0
        self.base_pv = 0
        self.num_buy = 0 
        self.num_sell = 0
        self.num_hold = 0
    
    def decide_action(self, env):
        # 환경 데이터로 부터 실제 액션의 결정을 위한 로직

        

        return action, confidence

