from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# 创建环境
sc = SparkContext(master="local[2]", appName="NetworkWordCount")
ssc = StreamingContext(sc, 3)  # 每隔一秒钟监听一次数据

# 设置检测点
ssc.checkpoint("checkpoint")

# 状态更新函数
def updatefun(new_values,last_sum):
    
    # 向前转态加上当前状态的key状态的value值
    return sum(new_values) + (last_sum or 0)

# 监听端口数据
lines = ssc.socketTextStream("localhost", 9999)
#　拆分单词
words = lines.flatMap(lambda line: line.split(" "))
#  统计词频
pairs = words.map(lambda word:(word, 1))
wordCounts = pairs.updateStateByKey(updateFunc=updatefun)
wordCounts.pprint()

ssc.start()
ssc.awaitTermination()