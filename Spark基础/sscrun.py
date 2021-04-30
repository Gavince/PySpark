
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# 创建环境
sc = SparkContext(master="local[2]", appName="NetworkWordCount")
ssc = StreamingContext(sc, 1)  # 每隔一秒钟监听一次数据

# 监听端口数据
lines = ssc.socketTextStream("localhost", 9999)
#　拆分单词
words = lines.flatMap(lambda line: line.split(" "))
# 统计词频
pairs = words.map(lambda word:(word, 1))
wordCounts = pairs.reduceByKey(lambda x, y:x+y)
# 打印数据
wordCounts.pprint()

# 开启流式处理
ssc.start()
ssc.awaitTermination()