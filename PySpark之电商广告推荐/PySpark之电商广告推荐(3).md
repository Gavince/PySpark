## 基于LR的点击率预测模型训练

### 整体流程

- raw_sample.csv ==> 历史样本数据
- ad_feature.csv ==> 广告特征数据
- user_profile.csv ==> 用户特征数据
- raw_sample.csv + ad_feature.csv + user_profile.csv ==> CTR点击率预测模型

### 数据处理

#### raw_sample表

**数据读入与标准化**

```python
# 数据读取
_raw_sample_df1 = spark.read.csv("./data/raw_sample.csv", header=True)

# 更改表结构，转换为对应的数据类型
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, LongType, StringType
  
# 更改df表结构：更改列类型和列名称
_raw_sample_df2 = _raw_sample_df1.\
    withColumn("user", _raw_sample_df1.user.cast(IntegerType())).withColumnRenamed("user", "userId").\
    withColumn("time_stamp", _raw_sample_df1.time_stamp.cast(LongType())).withColumnRenamed("time_stamp", "timestamp").\
    withColumn("adgroup_id", _raw_sample_df1.adgroup_id.cast(IntegerType())).withColumnRenamed("adgroup_id", "adgroupId").\
    withColumn("pid", _raw_sample_df1.pid.cast(StringType())).\
    withColumn("nonclk", _raw_sample_df1.nonclk.cast(IntegerType())).\
    withColumn("clk", _raw_sample_df1.clk.cast(IntegerType()))
    
# 样本数据pid特征处理
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline

stringindexer = StringIndexer(inputCol='pid', outputCol='pid_feature')
encoder = OneHotEncoder(dropLast=False, inputCol='pid_feature', outputCol='pid_value')
pipeline = Pipeline(stages=[stringindexer, encoder])
pipeline_fit = pipeline.fit(_raw_sample_df2)
raw_sample_df = pipeline_fit.transform(_raw_sample_df2)

raw_sample_df.show(5)
```

```python
+------+----------+---------+-----------+------+---+-----------+-------------+
|userId| timestamp|adgroupId|        pid|nonclk|clk|pid_feature|    pid_value|
+------+----------+---------+-----------+------+---+-----------+-------------+
|581738|1494137644|        1|430548_1007|     1|  0|        0.0|(2,[0],[1.0])|
|449818|1494638778|        3|430548_1007|     1|  0|        0.0|(2,[0],[1.0])|
|914836|1494650879|        4|430548_1007|     1|  0|        0.0|(2,[0],[1.0])|
|914836|1494651029|        5|430548_1007|     1|  0|        0.0|(2,[0],[1.0])|
|399907|1494302958|        8|430548_1007|     1|  0|        0.0|(2,[0],[1.0])|
+------+----------+---------+-----------+------+---+-----------+-------------+
only showing top 5 rows
```

#### ad_feature表

**数据读入与标准化**

```python
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

# 更改表结构，转换为对应的数据类型
_ad_feature_df = spark.read.csv("./data/ad_feature.csv", header=True)

# 替换掉NULL字符串
_ad_feature_df = _ad_feature_df.replace("NULL", "-1")

# 更改df表结构：更改列类型和列名称
ad_feature_df = _ad_feature_df.\
    withColumn("adgroup_id", _ad_feature_df.adgroup_id.cast(IntegerType())).withColumnRenamed("adgroup_id", "adgroupId").\
    withColumn("cate_id", _ad_feature_df.cate_id.cast(IntegerType())).withColumnRenamed("cate_id", "cateId").\
    withColumn("campaign_id", _ad_feature_df.campaign_id.cast(IntegerType())).withColumnRenamed("campaign_id", "campaignId").\
    withColumn("customer", _ad_feature_df.customer.cast(IntegerType())).withColumnRenamed("customer", "customerId").\
    withColumn("brand", _ad_feature_df.brand.cast(IntegerType())).withColumnRenamed("brand", "brandId").\
    withColumn("price", _ad_feature_df.price.cast(FloatType()))

ad_feature_df.printSchema()
ad_feature_df.show(5)
```

```python
root
 |-- adgroupId: integer (nullable = true)
 |-- cateId: integer (nullable = true)
 |-- campaignId: integer (nullable = true)
 |-- customerId: integer (nullable = true)
 |-- brandId: integer (nullable = true)
 |-- price: float (nullable = true)

+---------+------+----------+----------+-------+-----+
|adgroupId|cateId|campaignId|customerId|brandId|price|
+---------+------+----------+----------+-------+-----+
|    63133|  6406|     83237|         1|  95471|170.0|
|   313401|  6406|     83237|         1|  87331|199.0|
|   248909|   392|     83237|         1|  32233| 38.0|
|   208458|   392|     83237|         1| 174374|139.0|
|   110847|  7211|    135256|         2| 145952|32.99|
+---------+------+----------+----------+-------+-----+
only showing top 5 rows
```

#### user_profile表

**数据读入与标准化**

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, FloatType

# 构建表结构schema对象
schema = StructType([
    StructField("userId", IntegerType()),
    StructField("cms_segid", IntegerType()),
    StructField("cms_group_id", IntegerType()),
    StructField("final_gender_code", IntegerType()),
    StructField("age_level", IntegerType()),
    StructField("pvalue_level", IntegerType()),
    StructField("shopping_level", IntegerType()),
    StructField("occupation", IntegerType()),
    StructField("new_user_class_level", IntegerType())
])

_user_profile_df1 = spark.read.csv("./data/user_profile.csv", header=True, schema=schema)
# 填补缺失值
_user_profile_df2 = _user_profile_df1.na.fill(-1)

# 热编码时，必须先将待处理字段转为字符串类型才可处理(这两个特征增加了一个新的空值类别)
_user_profile_df3 = _user_profile_df2.withColumn("pvalue_level", _user_profile_df2.pvalue_level.cast(StringType()))\
    .withColumn("new_user_class_level", _user_profile_df2.new_user_class_level.cast(StringType()))
    
# 对pvalue_level进行热编码，也即一种缺失值填充
# 运行过程是先将pvalue_level转换为一列新的特征数据，然后对该特征数据求出的热编码值，存在了新的一列数据中，类型为一个稀疏矩阵
stringindexer = StringIndexer(inputCol='pvalue_level', outputCol='pl_onehot_feature')
encoder = OneHotEncoder(dropLast=False, inputCol='pl_onehot_feature', outputCol='pl_onehot_value')
pipeline = Pipeline(stages=[stringindexer, encoder])
pipeline_fit = pipeline.fit(_user_profile_df3)
_user_profile_df4 = pipeline_fit.transform(_user_profile_df3)

# 使用热编码转换new_user_class_level的一维数据为多维
stringindexer = StringIndexer(inputCol='new_user_class_level', outputCol='nucl_onehot_feature')
encoder = OneHotEncoder(dropLast=False, inputCol='nucl_onehot_feature', outputCol='nucl_onehot_value')
pipeline = Pipeline(stages=[stringindexer, encoder])
pipeline_fit = pipeline.fit(_user_profile_df4)
user_profile_df = pipeline_fit.transform(_user_profile_df4)
user_profile_df.printSchema()
user_profile_df.show(5)
```

```python
root
 |-- userId: integer (nullable = true)
 |-- cms_segid: integer (nullable = true)
 |-- cms_group_id: integer (nullable = true)
 |-- final_gender_code: integer (nullable = true)
 |-- age_level: integer (nullable = true)
 |-- pvalue_level: string (nullable = true)
 |-- shopping_level: integer (nullable = true)
 |-- occupation: integer (nullable = true)
 |-- new_user_class_level: string (nullable = true)
 |-- pl_onehot_feature: double (nullable = false)
 |-- pl_onehot_value: vector (nullable = true)
 |-- nucl_onehot_feature: double (nullable = false)
 |-- nucl_onehot_value: vector (nullable = true)

+------+---------+------------+-----------------+---------+------------+--------------+----------+--------------------+-----------------+---------------+-------------------+-----------------+
|userId|cms_segid|cms_group_id|final_gender_code|age_level|pvalue_level|shopping_level|occupation|new_user_class_level|pl_onehot_feature|pl_onehot_value|nucl_onehot_feature|nucl_onehot_value|
+------+---------+------------+-----------------+---------+------------+--------------+----------+--------------------+-----------------+---------------+-------------------+-----------------+
|   234|        0|           5|                2|        5|          -1|             3|         0|                   3|              0.0|  (4,[0],[1.0])|                2.0|    (5,[2],[1.0])|
|   523|        5|           2|                2|        2|           1|             3|         1|                   2|              2.0|  (4,[2],[1.0])|                1.0|    (5,[1],[1.0])|
|   612|        0|           8|                1|        2|           2|             3|         0|                  -1|              1.0|  (4,[1],[1.0])|                0.0|    (5,[0],[1.0])|
|  1670|        0|           4|                2|        4|          -1|             1|         0|                  -1|              0.0|  (4,[0],[1.0])|                0.0|    (5,[0],[1.0])|
|  2545|        0|          10|                1|        4|          -1|             3|         0|                  -1|              0.0|  (4,[0],[1.0])|                0.0|    (5,[0],[1.0])|
+------+---------+------------+-----------------+---------+------------+--------------+----------+--------------------+-----------------+---------------+-------------------+-----------------+
only showing top 5 rows
```

**查看编码后的对应关系**

```python
# 查看标签转换后的对应关系(注意:不是一一对应关系)
user_profile_df.groupBy("pvalue_level").min("pl_onehot_feature").show()
user_profile_df.groupBy("new_user_class_level").min("nucl_onehot_feature").show()
```

```python
+------------+----------------------+
|pvalue_level|min(pl_onehot_feature)|
+------------+----------------------+
|          -1|                   0.0|
|           3|                   3.0|
|           1|                   2.0|
|           2|                   1.0|
+------------+----------------------+
+--------------------+------------------------+
|new_user_class_level|min(nucl_onehot_feature)|
+--------------------+------------------------+
|                  -1|                     0.0|
|                   3|                     2.0|
|                   1|                     4.0|
|                   4|                     3.0|
|                   2|                     1.0|
+--------------------+------------------------+
```

#### 合并表

```python
# 数据合并
condition = [raw_sample_df.adgroupId == ad_feature_df.adgroupId]
_ = raw_sample_df.join(ad_feature_df, on=condition, how="outer")

condition2 = [_.userId==user_profile_df.userId]
datasets = _.join(user_profile_df, condition2, "outer")

datasets.printSchema()
```

```python
root
 |-- userId: integer (nullable = true)
 |-- timestamp: long (nullable = true)
 |-- adgroupId: integer (nullable = true)
 |-- pid: string (nullable = true)
 |-- nonclk: integer (nullable = true)
 |-- clk: integer (nullable = true)
 |-- pid_feature: double (nullable = true)
 |-- pid_value: vector (nullable = true)
 |-- adgroupId: integer (nullable = true)
 |-- cateId: integer (nullable = true)
 |-- campaignId: integer (nullable = true)
 |-- customerId: integer (nullable = true)
 |-- brandId: integer (nullable = true)
 |-- price: float (nullable = true)
 |-- userId: integer (nullable = true)
 |-- cms_segid: integer (nullable = true)
 |-- cms_group_id: integer (nullable = true)
 |-- final_gender_code: integer (nullable = true)
 |-- age_level: integer (nullable = true)
 |-- pvalue_level: string (nullable = true)
 |-- shopping_level: integer (nullable = true)
 |-- occupation: integer (nullable = true)
 |-- new_user_class_level: string (nullable = true)
 |-- pl_onehot_feature: double (nullable = true)
 |-- pl_onehot_value: vector (nullable = true)
 |-- nucl_onehot_feature: double (nullable = true)
 |-- nucl_onehot_value: vector (nullable = true)
```

### 建模

#### LR模型（未对名义变量进行one hot编码）

```python
# 剔除冗余、不需要的字段
useful_cols = [
    # 时间字段，划分训练集和测试集
    "timestamp",
    
    # label目标值字段
    "clk",  
    
    # 特征值字段
    "pid_value",       # 资源位的特征向量
    "price",    # 广告价格
    
    "cms_segid",    # 用户微群ID
    "cms_group_id",    # 用户组ID
    "final_gender_code",    # 用户性别特征，[1,2]
    "age_level",    # 年龄等级，1-
    "shopping_level",
    "occupation",
    
    "pl_onehot_value",
    "nucl_onehot_value"
]

# datasets_1已选择的特征
datasets_1 = datasets.select(*useful_cols)
datasets_1 = datasets_1.dropna()  # 消除外连接表后所产生的空值

from pyspark.ml.feature import VectorAssembler

datasets_1 = VectorAssembler().setInputCols(useful_cols[2:]).setOutputCol("features").transform(datasets_1)

# 以时间为划分线,划分测试集和训练集(最后一天为测试集)
train_datasets = datasets_1.filter(datasets_1.timestamp <= (1494691186-24*60*60))
test_datasets = datasets_1.filter(datasets_1.timestamp > (1494691186-24*60*60))

# LR模型预测
from pyspark.ml.classification import LogisticRegression

lr = LogisticRegression(labelCol="clk", featuresCol="features")
model = lr.fit(train_datasets)

result = model.transform(test_datasets)
result.select("clk", "price", "probability", "prediction").sort("probability").show(10)
```

```python
+---+-----------+--------------------+----------+
|clk|      price|         probability|prediction|
+---+-----------+--------------------+----------+
|  0|      1.0E8|[0.86822033579080...|       0.0|
|  0|      1.0E8|[0.88410456898285...|       0.0|
|  0|      1.0E8|[0.89175497524430...|       0.0|
|  1|5.5555556E7|[0.92481456348734...|       0.0|
|  0|      1.5E7|[0.93741450416609...|       0.0|
|  0|      1.5E7|[0.93757135053513...|       0.0|
|  0|      1.5E7|[0.93834723068486...|       0.0|
|  0|     1099.0|[0.93972095737025...|       0.0|
|  0|      338.0|[0.93972135016259...|       0.0|
|  0|      311.0|[0.93972136409867...|       0.0|
+---+-----------+--------------------+----------+
only showing top 10 rows
```

```python
result.filter(result.clk == 1).select("clk", "price", "probability", "prediction").sort("probability").show(20)
```

```python
+---+-----------+--------------------+----------+
|clk|      price|         probability|prediction|
+---+-----------+--------------------+----------+
|  1|5.5555556E7|[0.92481456348734...|       0.0|
|  1|      138.0|[0.93972145339276...|       0.0|
|  1|       35.0|[0.93972150655624...|       0.0|
|  1|      149.0|[0.93999389734424...|       0.0|
|  1|     5608.0|[0.94001892235042...|       0.0|
|  1|      275.0|[0.94002166220536...|       0.0|
|  1|       35.0|[0.94002178550379...|       0.0|
|  1|       49.0|[0.94004219522449...|       0.0|
|  1|      915.0|[0.94021082866896...|       0.0|
|  1|      598.0|[0.94021099104461...|       0.0|
|  1|      568.0|[0.94021100641137...|       0.0|
|  1|      398.0|[0.94021109348960...|       0.0|
|  1|      368.0|[0.94021110885634...|       0.0|
|  1|      299.0|[0.94021114419981...|       0.0|
|  1|      278.0|[0.94021115495652...|       0.0|
|  1|      259.0|[0.94021116468878...|       0.0|
|  1|      258.0|[0.94021116520100...|       0.0|
|  1|      258.0|[0.94021116520100...|       0.0|
|  1|      258.0|[0.94021116520100...|       0.0|
|  1|      195.0|[0.94021119747110...|       0.0|
+---+-----------+--------------------+----------+
only showing top 20 rows
```

#### LR模型（one hot编码）

<font color="blue">**分析**：</font>

训练CTRModel_AllOneHot

- "pid_value", 类别型特征，已被转换为多维特征==> 2维
- "price", 统计型特征 ===> 1维
- "cms_segid", 类别型特征，约97个分类 ===> 1维
- "cms_group_id", 类别型特征，约13个分类 ==> 1维
- "final_gender_code", 类别型特征，2个分类 ==> 1维
- "age_level", 类别型特征，7个分类 ==> 1维
- "shopping_level", 类别型特征，3个分类 ==> 1维
- "occupation", 类别型特征，2个分类 ==> 1维
- "pl_onehot_value", 类别型特征，已被转换为多维特征 ==> 4维
- "nucl_onehot_value" 类别型特征，已被转换为多维特征 ==> 5维

类别性特征都可以考虑进行热独编码，**将单一变量变为多变量，相当于增加了相关特征的数量**

- "cms_segid", 类别型特征，约97个分类 ===> 97维 舍弃
- "cms_group_id", 类别型特征，约13个分类 ==> 13维
- "final_gender_code", 类别型特征，2个分类 ==> 2维
- "age_level", 类别型特征，7个分类 ==>7维
- "shopping_level", 类别型特征，3个分类 ==> 3维
- "occupation", 类别型特征，2个分类 ==> 2维

但由于cms_segid分类过多，这里考虑舍弃，避免数据过于稀疏

##### one hot编码

```python
# 构造onehot编码,首先将相对应的特征转换为字符串类型的

datasets_2 = datasets.withColumn("cms_group_id", datasets.cms_group_id.cast(StringType()))\
    .withColumn("final_gender_code", datasets.final_gender_code.cast(StringType()))\
    .withColumn("age_level", datasets.age_level.cast(StringType()))\
    .withColumn("shopping_level", datasets.shopping_level.cast(StringType()))\
    .withColumn("occupation", datasets.occupation.cast(StringType()))
    
useful_cols_2 = [
    # 时间值，划分训练集和测试集
    "timestamp",
    
    # label目标值
    "clk", 
    
    # 特征值
    "price",
    "cms_group_id",
    "final_gender_code",
    "age_level",
    "shopping_level",
    "occupation",
    "pid_value", 
    "pl_onehot_value",
    "nucl_onehot_value"
]

datasets_2 = datasets_2.select(*useful_cols_2)
datasets_2 = datasets_2.dropna()

# 热编码处理函数封装
def oneHotEncoder(col1, col2, col3, data):
    
    stringindexer = StringIndexer(inputCol=col1, outputCol=col2)
    encoder = OneHotEncoder(dropLast=False, inputCol=col2, outputCol=col3)
    pipeline = Pipeline(stages=[stringindexer, encoder])
    pipeline_fit = pipeline.fit(data)
    
    return pipeline_fit.transform(data)

# 对这五个字段进行热独编码
#     "cms_group_id",
#     "final_gender_code",
#     "age_level",
#     "shopping_level",
#     "occupation",

datasets_2 = oneHotEncoder("cms_group_id", "cms_group_id_feature", "cms_group_id_value", datasets_2)
datasets_2 = oneHotEncoder("final_gender_code", "final_gender_code_feature", "final_gender_code_value", datasets_2)
datasets_2 = oneHotEncoder("age_level", "age_level_feature", "age_level_value", datasets_2)
datasets_2 = oneHotEncoder("shopping_level", "shopping_level_feature", "shopping_level_value", datasets_2)
datasets_2 = oneHotEncoder("occupation", "occupation_feature", "occupation_value", datasets_2)
```

```python
## 查看编码一一对应关系
datasets_2.groupBy("cms_group_id").min("cms_group_id_feature").show()
# datasets_2.groupBy("final_gender_code").min("final_gender_code_feature").show()
# datasets_2.groupBy("age_level").min("age_level_feature").show()
# datasets_2.groupBy("shopping_level").min("shopping_level_feature").show()
# datasets_2.groupBy("occupation").min("occupation_feature").show()
```

```python
+------------+-------------------------+
|cms_group_id|min(cms_group_id_feature)|
+------------+-------------------------+
|           7|                      9.0|
|          11|                      6.0|
|           3|                      0.0|
|           8|                      8.0|
|           0|                     12.0|
|           5|                      3.0|
|           6|                     10.0|
|           9|                      5.0|
|           1|                      7.0|
|          10|                      4.0|
|           4|                      1.0|
|          12|                     11.0|
|           2|                      2.0|
+------------+-------------------------+
```

##### 建模

```python
# 由于热独编码后，特征字段不再是之前的字段，重新定义特征值字段
feature_cols = [
    # 特征值
    "price",
    
    "cms_group_id_value",
    "final_gender_code_value",
    "age_level_value",
    "shopping_level_value",
    "occupation_value",
    
    "pid_value",
    "pl_onehot_value",
    "nucl_onehot_value"
]

from pyspark.ml.feature import VectorAssembler

datasets_2 = VectorAssembler(inputCols=feature_cols, outputCol="features").transform(datasets_2)
# 划分数据集
train_datasets2 = datasets_2.filter(datasets_2.timestamp<=(1494691186-24*60*60))
test_datasets2 = datasets_2.filter(datasets_2.timestamp>(1494691186-24*60*60))
print("train_datasets2:", train_datasets2.select("clk", "features").show(5, truncate=False))

# 模型training
lr2 = LogisticRegression(featuresCol="features", labelCol="clk")
model2 = lr2.fit(train_datasets2)
result2 = model2.transform(test_datasets2)
print("result1", result2.select("clk", "price", "probability", "prediction").sort("probability").show(20))
```

```python
train_datasets2:
+---+------------------------------------------------------------------------+
|clk|features                                                                |
+---+------------------------------------------------------------------------+
|0  |(39,[0,7,15,18,23,26,29,30,35],[108.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0]) |
|0  |(39,[0,7,15,18,23,26,29,30,35],[1880.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0])|
|1  |(39,[0,1,14,16,23,26,29,31,35],[1990.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0])|
|0  |(39,[0,1,14,16,23,26,29,31,35],[2200.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0])|
|0  |(39,[0,1,14,16,23,26,29,31,35],[5649.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0])|
+---+------------------------------------------------------------------------+
only showing top 5 rows
result1:
+---+-----------+--------------------+----------+
|clk|      price|         probability|prediction|
+---+-----------+--------------------+----------+
|  0|      1.0E8|[0.85524418892857...|       0.0|
|  0|      1.0E8|[0.88353143762124...|       0.0|
|  0|      1.0E8|[0.89169808985616...|       0.0|
|  1|5.5555556E7|[0.92511743960350...|       0.0|
|  0|     179.01|[0.93239951738308...|       0.0|
|  1|      159.0|[0.93239952905660...|       0.0|
|  0|      118.0|[0.93239955297535...|       0.0|
|  0|      688.0|[0.93451506165953...|       0.0|
|  0|      339.0|[0.93451525933626...|       0.0|
|  0|      335.0|[0.93451526160190...|       0.0|
|  0|      220.0|[0.93451532673881...|       0.0|
|  0|      176.0|[0.93451535166074...|       0.0|
|  0|      158.0|[0.93451536185607...|       0.0|
|  0|      158.0|[0.93451536185607...|       0.0|
|  1|      149.0|[0.93451536695374...|       0.0|
|  0|      122.5|[0.93451538196353...|       0.0|
|  0|       99.0|[0.93451539527410...|       0.0|
|  0|       88.0|[0.93451540150458...|       0.0|
|  0|       79.0|[0.93451540660224...|       0.0|
|  0|       75.0|[0.93451540886787...|       0.0|
+---+-----------+--------------------+----------+
only showing top 20 rows
```

### 参考

[推荐系统](https://www.jiqizhixin.com/graph/technologies/6ca1ea2d-6bca-45b7-9c93-725d288739c3)

[黑马python5.0](http://www.itheima.com/special/pythonzly/)

[推荐系统（一）：个性化电商广告推荐系统介绍、数据集介绍、项目效果展示、项目实现分析、点击率预测(CTR--Click-Through-Rate)概念](https://blog.csdn.net/qq_35456045/article/details/104881691?ops_request_misc=%257B%2522request%255Fid%2522%253A%2522159935430019724839860552%2522%252C%2522scm%2522%253A%252220140713.130102334.pc%255Fall.%2522%257D&request_id=159935430019724839860552&biz_id=0&utm_medium=distribute.pc_search_result.none-task-blog-2~all~first_rank_ecpm_v3~rank_business_v1-6-104881691.ecpm_v3_rank_business_v1&utm_term=%E4%B8%AA%E6%80%A7%E5%8C%96%E7%94%B5%E5%95%86%E6%8E%A8%E8%8D%90&spm=1018.2118.3001.4187)