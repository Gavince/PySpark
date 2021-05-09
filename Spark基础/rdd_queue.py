import findspark  
findspark.init()
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql.session import SparkSession
import time

# 环境配置
conf = SparkConf().setAppName("RDD Queue").setMaster("local")
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 10)

# 创建RDD数据流
rddQueue = []
for i in range(5):
    rddQueue += [ssc.sparkContext.parallelize([j for j in range(i)], 10)]
    time.sleep(1)
    
inputStream = ssc.queueStream(rddQueue)
reduceedStream = inputStream.map(lambda x: (x%10, 1)).reduceByKey(lambda x, y: x + y)

reduceedStream.pprint()
ssc.start()
ssc.stop(stopSparkContext=True, stopGraceFully=True)
