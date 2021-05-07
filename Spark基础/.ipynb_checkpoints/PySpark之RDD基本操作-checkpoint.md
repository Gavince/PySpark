# PySpark之RDD基本操作
Spark是基于内存的计算引擎，它的计算速度非常快。但是仅仅只涉及到数据的计算，并没有涉及到数据的存储，但是，spark的缺点是：吃内存，不太稳定

总体而言，Spark采用RDD以后能够实现高效计算的主要原因如下：  
（1）高效的容错性。现有的分布式共享内存、键值存储、内存数据库等，为了实现容错，必须在集群节点之间进行数据复制或者记录日志，也就是在节点之间会发生大量的数据传输，这对于数据密集型应用而言会带来很大的开销。在RDD的设计中，数据只读，不可修改，如果需要修改数据，必须从父RDD转换到子RDD，由此在不同RDD之间建立了血缘关系。<font color="blue">所以，RDD是一种天生具有容错机制的特殊集合，不需要通过数据冗余的方式（比如检查点）实现容错，而只需通过RDD父子依赖（血缘）关系重新计算得到丢失的分区来实现容错，无需回滚整个系统，这样就避免了数据复制的高开销，而且重算过程可以在不同节点之间并行进行，实现了高效的容错。</font>此外，RDD提供的转换操作都是一些粗粒度的操作（比如map、filter和join），RDD依赖关系只需要记录这种粗粒度的转换操作，而不需要记录具体的数据和各种细粒度操作的日志（比如对哪个数据项进行了修改），这就大大降低了数据密集型应用中的容错开销；  
（2）中间结果持久化到内存。数据在内存中的多个RDD操作之间进行传递，不需要“落地”到磁盘上，避免了不必要的读写磁盘开销；  
（3）存放的数据可以是Java对象，避免了不必要的对象序列化和反序列化开销。  

## 入门(词频统计)

这个程序的执行过程如下：
*  创建这个Spark程序的执行上下文，即创建SparkContext对象；
*  从外部数据源或HDFS文件中读取数据创建words对象；
*  构建起words和RDD之间的依赖关系，形成DAG图，这时候并没有发生真正的计算，只是记录转换的轨迹；
*  执行到,collect()是一个行动类型的操作，触发真正的计算，开始实际执行从words到RDD的转换操作，并把结果持久化到内存中，最后计算出RDD中包含的元素个数


```python
# SparkContext作为Spark的程序入口
import findspark
findspark.init()
from pyspark.context import SparkContext, SparkConf
import os 
```


```python
sc = SparkContext()
# 输出Spark基本信息
sc
```

<div>
    <p><b>SparkContext</b></p>
**textFile的使用**:  
Spark采用textFile()方法来从文件系统中加载数据创建RDD,该方法把文件的URI作为参数，这个URI可以是：  
•本地文件系统的地址  
•或者是分布式文件系统HDFS的地址  
•或者是Amazon S3的地址等等  


```python
# 获取词信息(从本地文件系统中读取数据)
path = os.path.join(os.getcwd(), "data/text.txt")
words = sc.textFile(name="file://" + path, minPartitions=5)
words
```


```python
words.collect()
```


    ['Prior versions of the MovieLens dataset included either pre-computed cross-',
     'folds or scripts to perform this computation. We no longer bundle either of',
     'these features with the dataset, since most modern toolkits provide this as',
     ' a built-in feature. If you wish to learn about standard approaches to cross',
     '-fold computation in the context of recommender systems evaluation, see for',
     ' tools, documentation, and open-source code examples.']


```python
# 构建整体流程(只构建，不计算)
RDD = words.flatMap(lambda line:line.split(" ")).map(lambda x:(x, 1)).reduceByKey(lambda a, b: a + b)
```


```python
# 词典大小
len(RDD.collect())
```


    58


```python
#聚合操作，也即整合所有的流程得到输出结果
RDD.collect()[:10]
```


    [('of', 3),
     ('to', 3),
     ('no', 1),
     ('', 2),
     ('learn', 1),
     ('computation', 1),
     ('see', 1),
     ('code', 1),
     ('examples.', 1),
     ('versions', 1)]

## RDD的常用操作

### RDD Transformation算子
**目的**：从一个已经存在的数据集创建一个新的数据集  
**说明**：  
(1) 对于RDD而言，每一次转换操作都会产生不同的RDD，供给下一个“转换”使用.  
(2) 转换得到的RDD是惰性求值的，也就是说，整个转换过程只是记录了转换的轨迹，并不会发生真正的计算，只有遇到行动操作时，才会发生真正的计算，开始从血缘关系源头开始，进行物理的转换操作．  
**常用转换操作**：  
![](./imgs/transform.png)


```python
# map(fun)
rdd1 = sc.parallelize([1, 2, 3, 5, 6, 7, 8], 5)
rdd2 = rdd1.map(lambda x: x+1)
rdd2.collect()
```


    [2, 3, 4, 6, 7, 8, 9]


```python
# filter(fun)
rdd3 = rdd2.filter(lambda x:x>5)
rdd3.collect()
```


    [6, 7, 8, 9]


```python
# flatMap操作, 先执行map操作，后执行flatc操作
rdd4 = sc.parallelize(["a b x", "d u j", "l p o"], 4)
rdd5 = rdd4.flatMap(lambda x: x.split(" "))
rdd6 = rdd4.map(lambda x:x.split(" "))
print(rdd5.collect(), rdd6.collect())
```

    ['a', 'b', 'x', 'd', 'u', 'j', 'l', 'p', 'o'] [['a', 'b', 'x'], ['d', 'u', 'j'], ['l', 'p', 'o']]

```python
# union 求并集
rdd1 = sc.parallelize([("a",1),("b",2)])
rdd2 = sc.parallelize([("c",1),("b",3)])
rdd3 = rdd1.union(rdd2)
rdd3.collect()
```


    [('a', 1), ('b', 2), ('c', 1), ('b', 3)]


```python
# groupByKey [("b", [2, 3])]
rdd4 = rdd3.groupByKey()
rdd4.collect()
```


    [('a', <pyspark.resultiterable.ResultIterable at 0x7f86f8df02b0>),
     ('b', <pyspark.resultiterable.ResultIterable at 0x7f86f8d8ab50>),
     ('c', <pyspark.resultiterable.ResultIterable at 0x7f86f8d8afd0>)]


```python
list(rdd4.collect()[1][1])
```


    [2, 3]


```python
# reduceByKey，相同key值的value值相加
rdd5 = rdd3.reduceByKey(lambda x, y:x+y)
rdd5.collect()
```


    [('a', 1), ('b', 5), ('c', 1)]


```python
# sortByKey
temp = [('Mary', 1), ('had', 2), ('a', 3), ('little', 4), ('lamb', 5)]
rdd = sc.parallelize(temp, 5)
rdd1 = rdd.sortByKey()
rdd1.collect()
```


    [('Mary', 1), ('a', 3), ('had', 2), ('lamb', 5), ('little', 4)]


```python
# sortBy
rdd2 = rdd.sortBy(keyfunc=lambda x:x[1], ascending=False)
rdd2.collect()
```


    [('lamb', 5), ('little', 4), ('a', 3), ('had', 2), ('Mary', 1)]

### RDD Action算子
**说明**：  
行动操作是真正触发计算的地方。Spark程序执行到行动操作时，才会执行真正的计算，从文件中加载数据，完成一次又一次转换操作，最终，完成行动操作得到结果。  
**惰性机制**:  
所谓的“惰性机制”是指，整个转换过程只是记录了转换的轨迹，并不会发生真正的计算，只有遇到行动操作时，才会触发“从头到尾”的真正的计算。  
**常用动作操作**:  
![](./imgs/action.png)


```python
rdd.collect()
```


    [('Mary', 1), ('had', 2), ('a', 3), ('little', 4), ('lamb', 5)]


```python
# first
rdd.first()
```


    ('Mary', 1)


```python
# take(显示指定个数的元素)
rdd.take(2)
```


    [('Mary', 1), ('had', 2)]


```python
rdd.top(2)
```


    [('little', 4), ('lamb', 5)]


```python
# count
rdd.count()
```


    5


```python
rdd.foreach(lambda x: print(x))
```


```python
# reduce累加
rdd_re = sc.parallelize([2, 3, 5, 4])
rdd_re.reduce(lambda a, b: a + b)
```


    14

### 持久化
**持久化的由来**:  
在Spark中，RDD采用惰性求值的机制，每次遇到行动操作，都会从头开始执行计算。每次调用行动操作，都会触发一次从头开始的计算。这对于迭代计算而言，代价是很大的，迭代计算经常需要多次重复使用同一组数据,因此将元素多次使用的RDD保存到内存中，可以加快计算速度。  
**说明**：  
- 可以通过持久化（缓存）机制避免这种重复计算的开销
- 可以使用persist()方法对一个RDD标记为持久化
- 之所以说“标记为持久化”，是因为出现persist()语句的地方，并不会马上计算生成RDD并把它持久化，而是要等到遇到第一个行动操作触发真正计算以后，才会把计算结果进行持久化
- 持久化后的RDD将会被保留在计算节点的内存中被后面的行动操作重复使用  
**常用操作**：  
![](./imgs/persist.png)



```python
name_array = ["Hadoop", "Spark", "Hive"]
rdd = sc.parallelize(name_array)
# 数据缓存到内存中，在遇见第一个行动操作时执行(可以使用rdd.cache() 保存到内存中)
rdd.persist()
print(rdd.count())  # 从头到尾执行
```

    3

```python
print(",".join(rdd.collect()))  # 直接使用缓存在内存中的数据
```

    Hadoop,Spark,Hive

```python
# 不在使用时,从内存中释放
rdd.unpersist()
```


    ParallelCollectionRDD[44] at readRDDFromFile at PythonRDD.scala:274

### 分区
**分区说明**:  
RDD是弹性分布式数据集，通常RDD很大，会被分成很多个分区，分别保存在不同的节点上。  
**分区的优点**:  
（１）增加并行度  
（２）减少通讯开销


```python
# 分区
data = sc.parallelize([1, 2, 3, 4, 5], 5)
data.glom().collect()
```


    [[1], [2], [3], [4], [5]]


```python
# 重新分区
rdd = data.repartition(2)
rdd.glom().collect()
```


    [[1, 3, 4], [2, 5]]


```python
rdd1 = data.map(lambda x:(x, 1))
rdd1.glom().collect()
```


    [[(1, 1)], [(2, 1)], [(3, 1)], [(4, 1)], [(5, 1)]]


```python
rdd2 = rdd1.partitionBy(2)
```


```python
rdd2.glom().collect()
```


    [[(2, 1), (4, 1)], [(1, 1), (3, 1), (5, 1)]]


```python
# 自定义分区

class SelfPartition:
    
    def myPartition(self, key):
        """自定义分区"""
        
        print("My partition is running!")
        print("The key is %d"%key)
        
        return key%10
    
    def main(self):
        """自定义分区函数"""

        print("The main funcation is running!")
        conf = SparkConf().setMaster("local").setAppName("MyApp")
        sc = SparkContext(conf=conf)
        
        data = sc.parallelize(range(10), 5)
        data.map(lambda x:(x, 1)).partitionBy(10, self.myPartition).map(lambda x:x[0]).saveAsTextFile("./data/pp")
```


```python
sp = SelfPartition()
sp.main()
```


## 实战１：通过Spark实现点击流日志分析

### 网络总访问量


```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("pv").getOrCreate()
sc = spark.sparkContext
path1 = os.path.join(os.getcwd(), "data/access.log")
```


```python
# 网络信息
rdd = sc.textFile("file://"+path1)
rdd.collect()[:5]
```


    ['194.237.142.21 - - [18/Sep/2013:06:49:18 +0000] "GET /wp-content/uploads/2013/07/rstudio-git3.png HTTP/1.1" 304 0 "-" "Mozilla/4.0 (compatible;)"',
     '183.49.46.228 - - [18/Sep/2013:06:49:23 +0000] "-" 400 0 "-" "-"',
     '163.177.71.12 - - [18/Sep/2013:06:49:33 +0000] "HEAD / HTTP/1.1" 200 20 "-" "DNSPod-Monitor/1.0"',
     '163.177.71.12 - - [18/Sep/2013:06:49:36 +0000] "HEAD / HTTP/1.1" 200 20 "-" "DNSPod-Monitor/1.0"',
     '101.226.68.137 - - [18/Sep/2013:06:49:42 +0000] "HEAD / HTTP/1.1" 200 20 "-" "DNSPod-Monitor/1.0"']


```python
# 每一行数据表示一次网站访问(访问量)
rdd1 = rdd.map(lambda x:("pv", 1)).reduceByKey(lambda a, b :a+b)
# rdd1.saveAsTextFile("data/pv.txt")
rdd1.collect()
```


    [('pv', 90)]

### 网站独立用户访问量


```python
spark = SparkSession.builder.appName("uv").getOrCreate()
sc = spark.sparkContext
path1 = os.path.join(os.getcwd(), "data/access.log")
```


```python
# 得到ip地址
rdd1 = sc.textFile("file://" + path1)
rdd2 = rdd1.map(lambda x:x.split(" ")).map(lambda x:x[0])
rdd2.collect()[:5]
```


    ['194.237.142.21',
     '183.49.46.228',
     '163.177.71.12',
     '163.177.71.12',
     '101.226.68.137']


```python
rdd3 = rdd2.distinct().map(lambda x:("uv", 1))
rdd4 = rdd3.reduceByKey(lambda a,b:a+b)
rdd4.collect()
# rdd4.saveAsTextFile("data/uv.txt")
```


    [('uv', 17)]

### 访问TopN


```python
spark = SparkSession.builder.appName("TopN").getOrCreate()
sc = spark.sparkContext
path1 = os.path.join(os.getcwd(), "data/access.log")
```


```python
rdd1 = sc.textFile("file://" + path1)
rdd2 = rdd1.map(lambda line:line.split(" ")).filter(lambda x:len(x)>10).map(lambda x:(x[10],1))
rdd3 = rdd2.reduceByKey(lambda a, b:a+b).sortBy(lambda x:x[1], ascending=False)
rdd4 = rdd3.take(5)
rdd4
```


    [('"-"', 27),
     ('"http://blog.fens.me/vps-ip-dns/"', 18),
     ('"http://blog.fens.me/wp-content/themes/silesia/style.css"', 7),
     ('"http://blog.fens.me/nodejs-socketio-chat/"', 7),
     ('"http://blog.fens.me/nodejs-grunt-intro/"', 7)]


```python
# 得到partion的分组情况
rdd3.glom().collect()
```


    [[('"-"', 27),
      ('"http://blog.fens.me/vps-ip-dns/"', 18),
      ('"http://blog.fens.me/wp-content/themes/silesia/style.css"', 7),
      ('"http://blog.fens.me/nodejs-socketio-chat/"', 7),
      ('"http://blog.fens.me/nodejs-grunt-intro/"', 7)],
     [('"http://blog.fens.me/nodejs-async/"', 5),
      ('"http://www.angularjs.cn/A00n"', 2),
      ('"http://cos.name/category/software/packages/"', 1),
      ('"http://blog.fens.me/series-nodejs/"', 1),
      ('"http://www.google.com/url?sa=t&rct=j&q=&esrc=s&source=web&cd=6&cad=rja&ved=0CHIQFjAF&url=http%3A%2F%2Fblog.fens.me%2Fvps-ip-dns%2F&ei=j045UrP5AYX22AXsg4G4DQ&usg=AFQjCNGsJfLMNZnwWXNpTSUl6SOEzfF6tg&sig2=YY1oxEybUL7wx3IrVIMfHA&bvm=bv.52288139,d.b2I"',
       1),
      ('"http://www.google.com/url?sa=t&rct=j&q=nodejs%20%E5%BC%82%E6%AD%A5%E5%B9%BF%E6%92%AD&source=web&cd=1&cad=rja&ved=0CCgQFjAA&url=%68%74%74%70%3a%2f%2f%62%6c%6f%67%2e%66%65%6e%73%2e%6d%65%2f%6e%6f%64%65%6a%73%2d%73%6f%63%6b%65%74%69%6f%2d%63%68%61%74%2f&ei=rko5UrylAefOiAe7_IGQBw&usg=AFQjCNG6YWoZsJ_bSj8kTnMHcH51hYQkAA&bvm=bv.52288139,d.aGc"',
       1),
      ('"http://www.angularjs.cn/"', 1)]]


```python
# 终止
sc.stop()
```

## 实战2：实现ip地址查询


```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("IPSearch").getOrCreate()
sc = spark.sparkContext
ip_path = os.path.join(os.getcwd(), "data/ip.txt")
```


```python
city_id_rdd = sc.textFile("file://" + ip_path)
city_id_rdd.take(2)
```


```python
# 起始地址 截止地址  经度 纬度
city_id_rdd = city_id_rdd.map(lambda x:x.split("|")).map(lambda x:(x[2], x[3],x[7], x[13], x[14]))
city_id_rdd.take(5)
```


    [('16777472', '16778239', '福州', '119.306239', '26.075302'),
     ('16779264', '16781311', '广州', '113.280637', '23.125178'),
     ('16785408', '16793599', '广州', '113.280637', '23.125178'),
     ('16842752', '16843007', '福州', '119.306239', '26.075302'),
     ('16843264', '16844799', '福州', '119.306239', '26.075302')]


```python
#　创建广播变量，使其不需要复制到每一个数据集当中
city_brodcast = sc.broadcast(city_id_rdd.collect())
city_brodcast.value[:2]  # 列表形式存取
```


    [('16777472', '16778239', '福州', '119.306239', '26.075302'),
     ('16779264', '16781311', '广州', '113.280637', '23.125178')]


```python
# 获得相对应的ip地址
dest_data = sc.textFile("data/20090121000132.394251.http.format").map(lambda x:x.split("|")[1])
dest_data.take(5)
```


    ['125.213.100.123',
     '117.101.215.133',
     '117.101.222.68',
     '115.120.36.118',
     '123.197.64.247']


```python
def ip_transform(ip):
    """ip地址装换"""
    
    ips = ip.split(".")#[223,243,0,0] 32位二进制数
    ip_num = 0
    for i in ips:
        ip_num = int(i) | ip_num << 8
    return ip_num
```


```python
def binary_search(ip_num, broadcast_value):
    start = 0
    end = len(broadcast_value) - 1
    while (start <= end):
        mid = int((start + end) / 2)
        if ip_num >= int(broadcast_value[mid][0]) and ip_num <= int(broadcast_value[mid][1]):
            return mid
        if ip_num < int(broadcast_value[mid][0]):
            end = mid
        if ip_num > int(broadcast_value[mid][1]):
            start = mid
```


```python
def get_pos(x):
    """ip地址"""
    
    city_brodcast_value = city_brodcast.value
    def get_result(ip):
        ip_num = ip_transform(ip)
        # index表示相对应的ip的位置信息的索引结点
        index = binary_search(ip_num, city_brodcast_value)
        
        # ((经度, 纬度, 地址), 1))
        return ((city_brodcast_value[index][2], city_brodcast_value[index][3], city_brodcast_value[index][4]), 1)
    
    x = map(tuple, [get_result(ip) for ip in x])
    return x
```


```python
# 按照Partitiion传入数据块进行运算
dest_rdd = dest_data.mapPartitions(lambda x:get_pos(x))
result_rdd = dest_rdd.reduceByKey(lambda a, b: a + b)
```


```python
# 统计相同地址经纬度下的sun
result_rdd.collect()
```


    [(('西安', '108.948024', '34.263161'), 1824),
     (('重庆', '107.7601', '29.32548'), 85),
     (('重庆', '106.504962', '29.533155'), 400),
     (('重庆', '106.27633', '29.97227'), 36),
     (('重庆', '107.39007', '29.70292'), 47),
     (('重庆', '106.56347', '29.52311'), 3),
     (('重庆', '107.08166', '29.85359'), 29),
     (('北京', '116.405285', '39.904989'), 1535),
     (('石家庄', '114.502461', '38.045474'), 383),
     (('重庆', '106.51107', '29.50197'), 91),
     (('昆明', '102.712251', '25.040609'), 126),
     (('重庆', '106.57434', '29.60658'), 177)]


```python
sc.stop()
```
## 实战3：Pair RDD操作
说明：统计每种图书每天的平均销量


```python
rdd = sc.parallelize([("Spark", 2), ("hadoop", 6), ("hadoop", 8), ("Spark", 5)])
rdd.collect()
```


    [('Spark', 2), ('hadoop', 6), ('hadoop', 8), ('Spark', 5)]


```python
rdd1 = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
rdd1.collect()
```


    [('hadoop', (14, 2)), ('Spark', (7, 2))]


```python
rdd1.mapValues(lambda x: x[1]/x[0]).collect()
```


    [('hadoop', 0.14285714285714285), ('Spark', 0.2857142857142857)]

## 实战4：求TOP
file文件有4列，求第三列前N个TOP值。


```python
import findspark
findspark.init()
import os
from pyspark import SparkContext, SparkConf
```


```python
# 路径
PATH = os.path.join(os.getcwd(), "data/top.txt")
# 配置运行环境
conf = SparkConf().setAppName("TOP").setMaster("local")
sc = SparkContext(conf=conf)
```


```python
lines = sc.textFile("file://" + PATH)
lines.collect()
```


    ['1,1768,50,155',
     '2,1218, 600,211',
     '3,2239,788,242',
     '4,3101,28,599',
     '5,4899,290,129',
     '6,3110,54,1201',
     '7,4436,259,877',
     '8,2369,7890,27']


```python
# 消除空格，并且一行满足四个特征
result1 = lines.filter(lambda line:(len(line.strip()) > 0) and (len(line.split(",")) == 4))
result2 = result1.map(lambda line: line.split(",")[2])
# 构建键值对元素
result3 = result2.map(lambda x: (int(x), ""))
# 整合为一个分区，全局最值
result4 = result3.repartition(1)
# 降序排列
result5 = result4.sortByKey(False)
result6 = result5.map(lambda x: x[0])
result6.take(5)
```


    [7890, 788, 600, 290, 259]

## 实战5：文件排序
读取文件中所有整数进行排序


```python
index = 0

PATH = os.path.join(os.getcwd(), "data/FileSort.txt")
PATH

def getIndex():
    
    global index
    index += 1

    return index
```


```python
def main(path):
    
    conf = SparkConf().setAppName("file_sort").setMaster("local")
    sc = SparkContext(conf=conf)
    rdd = sc.textFile("file://" + path)
    
    result1 = rdd.filter(lambda line: (len(line.strip()) > 0))
    # 构建键值对元素
    result2 = result1.map(lambda x: (int(x.strip()), ""))
    # 整合为一个分区，全局最值
    result3 = result2.repartition(1)
    # 降序排列
    result4 = result3.sortByKey(False)
    result5 = result4.map(lambda x: x[0])
    # rank
    result6 = result5.map(lambda x: (getIndex(), x))
    # 只包含文件目录
    result6.saveAsTextFile("file://" + os.getcwd() + "/data/FileSort")
    
    return result6
```


```python
res = main(PATH)
res.take(4)
```


    [(1, 4899), (2, 4436), (3, 3110), (4, 3101)]

## 实战6：二次排序
对于一个给定的文件（有两列整数），请对数据进行排序，首先根据第一列数据降序排序，如果第一列数据相等，则根据第二列数据降序排序
- 按照Ordered和Serializable接口实现自定义排序的key
- 将要进行二次排序的文件加载进来生成<key,value>类型的RDD
- 使用sortByKey基于自定义的key进行二次排序
- 去掉排序的key只保留排序的结果  
**解题思路：**  
![](./imgs/sort.png)


```python
from operator import gt


# 路径
PATH = os.path.join(os.getcwd(), "data/file.txt")
# 环境配置
conf = SparkConf().setAppName("spark_sort").setMaster("local")
sc = SparkContext(conf=conf)
```


```python
class SecondarySortKey():
    
    def __init__(self,k):
        self.column1 = k[0]
        self.column2 = k[1]

    def __gt__(self, other):

        if other.column1 ==self.column1:

            return gt(self.column2,other.column2)
        else:
            return gt(self.column1,other.column1)
```


```python
lines = sc.textFile("file://" + PATH)
lines.collect()
```


    ['50 155',
     '600 211',
     '788 242',
     '28 599',
     '290 129',
     '54 1201',
     '259 877',
     '7890 27']




```python
rdd1 = lines.filter(lambda x: (len(x.strip()) > 0))
# 构建可排序的key
rdd2 = rdd1.map(lambda x: ((int(x.split(" ")[0]), int(x.split(" ")[1])), x))
rdd2.collect()
```


    [((50, 155), '50 155'),
     ((600, 211), '600 211'),
     ((788, 242), '788 242'),
     ((28, 599), '28 599'),
     ((290, 129), '290 129'),
     ((54, 1201), '54 1201'),
     ((259, 877), '259 877'),
     ((7890, 27), '7890 27')]


```python
rdd3 = rdd2.map(lambda x: (SecondarySortKey(x[0]), x[1]))
rdd4 = rdd3.sortByKey(False)
rdd5 = rdd4.map(lambda x: x[1])
rdd5.collect()
```


    ['7890 27',
     '788 242',
     '600 211',
     '290 129',
     '259 877',
     '54 1201',
     '50 155',
     '28 599']

## 参考

[ubuntu18.04安装spark（伪分布式）](https://blog.csdn.net/weixin_42001089/article/details/82346367#commentBox)  
[jupyter－lab使用pyspark](https://blog.csdn.net/syani/article/details/72851425)