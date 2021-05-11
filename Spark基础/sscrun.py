
import findspark  
findspark.init()
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import pymysql
# 创建环境
sc = SparkContext(master="local[2]", appName="NetworkWordCount")
ssc = StreamingContext(sc, 1)  # 每隔一秒钟监听一次数据

# 监听端口数据(socket数据源)
lines = ssc.socketTextStream("localhost", 9999)
#　拆分单词
words = lines.flatMap(lambda line: line.split(" "))
# 统计词频
pairs = words.map(lambda word:(word, 1))
wordCounts = pairs.reduceByKey(lambda x, y:x+y)
# 打印数据
wordCounts.pprint()
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
wordCounts.foreachRDD(func)   
# 开启流式处理
ssc.start()
ssc.awaitTermination(timeout=10)
