
import findspark  
findspark.init()
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import os 
# 创建环境
sc = SparkContext(master="local[2]", appName="NetworkWordCounts")
ssc = StreamingContext(sc, 3)  # 每隔一秒钟监听一次数据
# 监听端口数据
lines = ssc.socketTextStream("localhost", 9999)
#　拆分单词
words = lines.flatMap(lambda line: line.split(" "))
words.reduceByKeyAndWindow??
