
import findspark  
findspark.init()
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql.session import SparkSession
import time
import os
import pymysql

# 环境配置
conf = SparkConf().setAppName("RDD Queue").setMaster("local")
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 2)


# 创建RDD数据流
rddQueue = []
for i in range(5):
    rddQueue += [ssc.sparkContext.parallelize([j for j in range(100 + i)], 10)]
    time.sleep(1)
    
inputStream = ssc.queueStream(rddQueue)
reduceedStream = inputStream.map(lambda x: (x%10, 1)).reduceByKey(lambda x, y: x + y)
# 方法一：保存值到指定文本文件中
# reduceedStream.saveAsTextFiles("file://" + os.getcwd() + "/process/") 
# 方法二：保存数据到mysql数据库中
def db_func(records):
    
    # 连接数据库
    # 连接mysql数据库
    print("正在连接数据库中!")
    db = pymysql.connect(host='localhost',user='root',passwd='123456',db='spark')
    cursor = db.cursor()
    def do_insert():
        # sql语句
        print("Inserting data!")
        sql = " insert into wordcount(word, count) values ('%s','%s')" % (str(p[0]), str(p[1]))
        try:
            cursor.ececute(sql)
            db.commit()
        except:
            # 回滚
            db.rollback()
        for item in recorders:
            do_insert(item)
            
def func(rdd):
    
    repartitionedRDD = rdd.repartition(3)
    repartitionedRDD.foreachPartition(db_func)

reduceedStream.pprint()
reduceedStream.foreachRDD(func)

ssc.start()
ssc.stop(stopSparkContext=True, stopGraceFully=True)
