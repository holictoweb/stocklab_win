from pyspark.sql import SparkSession
from pyspark.sql.functions import *


from stocklab.db_handler.mongodb_handler import MongoDBHandler



# moving average
from pyspark.sql.window import Window
from pyspark.sql import functions as func



'''
python -m stocklab.scheduler.data_spark_1d

한화솔루션 009830
'''


mongo = MongoDBHandler()
#spark session
#spark=(SparkSession.builder.appName("holicone").getOrCreate())


spark_60day = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri", "mongodb://172.22.144.1/stocklab.price_60day_info") \
    .config("spark.mongodb.output.uri", "mongodb://172.22.144.1/stocklab.price_60day_info") \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.0') \
    .getOrCreate()



#spark3
#org.mongodb.spark:mongo-spark-connector_2.12:3.0.0


def collect_min_mongodb():
    print ( "[collect_M] collect mongodb....")
    
    #chart_data = mongo.find_items({}, 'stocklab', 'price_60day_info')
    #print(chart_data)
    
    
    df_60day = spark_60day.read.format("mongo").load()
    #df_60day.printSchema()

    # Moving Average 5 10 15 30 60
    windowSpec_5 = Window.partitionBy("code").orderBy("날짜").rowsBetween(-5, 0)
    windowSpec_10 = Window.partitionBy("code").orderBy("날짜").rowsBetween(-10, 0)
    windowSpec_15 = Window.partitionBy("code").orderBy("날짜").rowsBetween(-15, 0)
    windowSpec_30 = Window.partitionBy("code").orderBy("날짜").rowsBetween(-30, 0)
    windowSpec_60 = Window.partitionBy("code").orderBy("날짜").rowsBetween(-60, 0)



    df_60day_map = df_60day.withColumn( '5map', avg("종가").over(windowSpec_5) ) \
        .withColumn( '10map', avg("종가").over(windowSpec_10) ) \
        .withColumn( '15map', avg("종가").over(windowSpec_15) ) \
        .withColumn( '30map', avg("종가").over(windowSpec_30) ) \
        .withColumn( '60map', avg("종가").over(windowSpec_60) ) 


    df_60day_map_mav = df_60day_map.withColumn( '5mav', avg("누적거래량").over(windowSpec_5) ) \
        .withColumn( '10mav', avg("누적거래량").over(windowSpec_10) ) \
        .withColumn( '15mav' , avg("누적거래량").over(windowSpec_15) ) \
        .withColumn( '30mav', avg("누적거래량").over(windowSpec_30) ) \
        .withColumn( '60mav', avg("누적거래량").over(windowSpec_60) ) 
    

    # moving average write
    df_60day_map_mav.write.format("parquet").mode("overwrite").save("/mnt/c/toone/pjttoone/data/price_60day_info.parquet")


    #read data
    '''
    df_test = spark.read.format("parquet").load("../data/price_60day_info.parquet")
    df_test.select("code", "날짜", "종가", '누적거래량',  "5map", "10map", "15map", "30map", "60map", "5mav", "10mav", "15mav", "30mav", "60mav").where(col("code") == "005990").orderBy(desc("날짜")).show(10)
    '''


if __name__ == "__main__":
    print("test")
    collect_ma_mongodb()