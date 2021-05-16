import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark import Row
from pyspark.sql.functions import split, length
from pyspark.sql.functions import explode
import os 

ROOT = "file://" + os.getcwd()


# 创建SparkSession对象
spark = SparkSession.builder.appName("StructuredNetworkWordCount").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
sc = spark.sparkContext

# 创建输入数据源
lines = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# 定义流计算过程
words = lines.select(explode(split(lines.value, " ")).alias("word"))
word_count = words.filter(length("word") == 5)

# 启动流计算并输出结果
query = word_count \
    .writeStream \
    .outputMode("complete") \
    .format("csv") \
    .option("path", ROOT + "/data/filesink") \
    .option("checkpointLocation", ROOT + "/data/file-sink-cp") \
    .trigger(processingTime="8 seconds") \
    .start()

query.awaitTermination()
