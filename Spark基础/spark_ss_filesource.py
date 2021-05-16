import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark import Row
from pyspark.sql.functions import split
from pyspark.sql.functions import explode
from pyspark.sql.types import StructField, StructType
from pyspark.sql.types import StringType, TimestampType
from pyspark.sql.functions import window, asc


if __name__ == "__main__":
    import os
    
    TEST_DATA_DIR_SPARK = "file://" + os.getcwd() + "/data/tmp/testdata/"
    schema = StructType([StructField("eventTime", TimestampType(), True)
                         , StructField("action", StringType(), True)
                         , StructField("district", StringType(), True)
                        ])

    # 创建SparkSession对象
    spark = SparkSession.builder.appName("StructuredEmallPurchaseCount.py").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext

    # 创建输入数据源
    lines = spark.readStream \
        .format("json") \
        .schema(schema) \
        .option("maxFilesPerTrigger", 100) \
        .load(TEST_DATA_DIR_SPARK)

    # 定义流计算过程
    # 定义窗口：在指定的窗口时间内进行统计
    windowDuration = "1 minutes"
    words = lines.filter(lines.action == "purchase").groupBy("district", window("eventTime", windowDuration)).count().sort(asc("window"))

    # 启动流计算并输出结果
    query = words \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="10 seconds") \
        .start()

    query.awaitTermination()
