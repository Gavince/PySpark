
import findspark  
findspark.init()
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql.session import SparkSession

def get_countryname(line):
    country_name = line.strip()  # 消除空格

    if country_name == 'usa':
        output = 'USA'
    elif country_name == 'ind':
        output = 'India'
    elif country_name == 'aus':
        output = 'Australia'
    else:
        output = 'Unknown'

    return (output, 1)

# 设置参数
batch_interval = 1
window_length = 6*batch_interval  # 窗口大小
frquency = 3*batch_interval  # 滑动频率

sc =  sc = SparkContext(master="local[2]", appName="NetworkWordCount")
ssc = StreamingContext(sc, batch_interval)

ssc.checkpoint("checkpoint")
# 监听端口数据
lines = ssc.socketTextStream("localhost", 9999)

addFunc = lambda x, y: x+y
invAddFunc = lambda x, y: x-y
word_counts = lines.map(get_countryname).reduceByKeyAndWindow(addFunc, invAddFunc, window_length, frquency)
word_counts.pprint()

ssc.start()
ssc.awaitTermination()
