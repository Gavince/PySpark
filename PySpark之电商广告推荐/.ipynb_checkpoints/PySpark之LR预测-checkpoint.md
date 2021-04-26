## PySpark之LR初探

### 环境配置

```python
import pandas as pd
import numpy as np
import pyspark
import os 
import datetime
import time
from pyspark import SparkConf
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import seaborn as sns

spark = SparkSession \
    .builder \
    .config('spark.executor.memory','16g')\  # 根据自己电脑配置,自行调节
    .config('spark.driver.memory','8g')\
    .config('spark.driver.maxResultsSize','0')\
    .getOrCreate()
```

### 数据获取

```python
# 样本数据集`
sample_dataset = [
    (0, "male", 37, 10, "no", 3, 18, 7, 4),
    (0, "female", 27, 4, "no", 4, 14, 6, 4),
    (0, "female", 32, 15, "yes", 1, 12, 1, 4),
    (0, "male", 57, 15, "yes", 5, 18, 6, 5),
    (0, "male", 22, 0.75, "no", 2, 17, 6, 3),
    (0, "female", 32, 1.5, "no", 2, 17, 5, 5),
    (0, "female", 22, 0.75, "no", 2, 12, 1, 3),
    (0, "male", 57, 15, "yes", 2, 14, 4, 4),
    (0, "female", 32, 15, "yes", 4, 16, 1, 2),
    (0, "male", 22, 1.5, "no", 4, 14, 4, 5),
    (0, "male", 37, 15, "yes", 2, 20, 7, 2),
    (0, "male", 27, 4, "yes", 4, 18, 6, 4),
    (0, "male", 47, 15, "yes", 5, 17, 6, 4),
    (0, "female", 22, 1.5, "no", 2, 17, 5, 4),
    (0, "female", 27, 4, "no", 4, 14, 5, 4),
    (0, "female", 37, 15, "yes", 1, 17, 5, 5),
    (0, "female", 37, 15, "yes", 2, 18, 4, 3),
    (0, "female", 22, 0.75, "no", 3, 16, 5, 4),
    (0, "female", 22, 1.5, "no", 2, 16, 5, 5),
    (0, "female", 27, 10, "yes", 2, 14, 1, 5),
    (1, "female", 32, 15, "yes", 3, 14, 3, 2),
    (1, "female", 27, 7, "yes", 4, 16, 1, 2),
    (1, "male", 42, 15, "yes", 3, 18, 6, 2),
    (1, "female", 42, 15, "yes", 2, 14, 3, 2),
    (1, "male", 27, 7, "yes", 2, 17, 5, 4),
    (1, "male", 32, 10, "yes", 4, 14, 4, 3),
    (1, "male", 47, 15, "yes", 3, 16, 4, 2),
    (0, "male", 37, 4, "yes", 2, 20, 6, 4)
]

columns = ["affairs", "gender", "age", "label", "children", "religiousness", "education", "occupation", "rating"]
```

```python
pdf = pd.DataFrame(sample_dataset, columns=columns) 
# 特征选取：affairs为目标值，其余为特征值
df1 = spark.createDataFrame(pdf)
df1.show(5)
```

```python
+-------+------+---+-----+--------+-------------+---------+----------+------+
|affairs|gender|age|label|children|religiousness|education|occupation|rating|
+-------+------+---+-----+--------+-------------+---------+----------+------+
|      0|  male| 37| 10.0|      no|            3|       18|         7|     4|
|      0|female| 27|  4.0|      no|            4|       14|         6|     4|
|      0|female| 32| 15.0|     yes|            1|       12|         1|     4|
|      0|  male| 57| 15.0|     yes|            5|       18|         6|     5|
|      0|  male| 22| 0.75|      no|            2|       17|         6|     3|
+-------+------+---+-----+--------+-------------+---------+----------+------+
only showing top 5 rows
```

### 数据预处理

- 对名义变量进行onehot编码

  ```python
  from pyspark.ml import Pipeline
  from pyspark.ml.feature import StringIndexer, OneHotEncoder
  
  
  def oneHotEncoder_fun(col1, col2, col3, data):
      """ 热编码处理函数封装 """
      
      stringindexer = StringIndexer(inputCol=col1, outputCol=col2)  # 字符转为index索引
      encoder = OneHotEncoder(dropLast=False, inputCol=col2, outputCol=col3)
      pipeline = Pipeline(stages=[stringindexer,  encoder])
      pipeline_fit = pipeline.fit(data)
      
      return pipeline_fit.transform(data)
  
  df1 = oneHotEncoder_fun("gender", "gender_feature", "gender_value",  df1)  # 必须保证相应的字段为字符串类型
  df1 = oneHotEncoder_fun("children", "children_feature", "children_value",  df1)
  df1.show(5)
  ```

  ```python
  +-------+------+---+-----+--------+-------------+---------+----------+------+--------------+-------------+----------------+--------------+
  |affairs|gender|age|label|children|religiousness|education|occupation|rating|gender_feature| gender_value|children_feature|children_value|
  +-------+------+---+-----+--------+-------------+---------+----------+------+--------------+-------------+----------------+--------------+
  |      0|  male| 37| 10.0|      no|            3|       18|         7|     4|           1.0|(2,[1],[1.0])|             1.0| (2,[1],[1.0])|
  |      0|female| 27|  4.0|      no|            4|       14|         6|     4|           0.0|(2,[0],[1.0])|             1.0| (2,[1],[1.0])|
  |      0|female| 32| 15.0|     yes|            1|       12|         1|     4|           0.0|(2,[0],[1.0])|             0.0| (2,[0],[1.0])|
  |      0|  male| 57| 15.0|     yes|            5|       18|         6|     5|           1.0|(2,[1],[1.0])|             0.0| (2,[0],[1.0])|
  |      0|  male| 22| 0.75|      no|            2|       17|         6|     3|           1.0|(2,[1],[1.0])|             1.0| (2,[1],[1.0])|
  +-------+------+---+-----+--------+-------------+---------+----------+------+--------------+-------------+----------------+--------------+
  only showing top 5 rows
  ```

  ```python
  # 查看变换后的数值对应关系
  df1.groupBy("gender").min("gender_feature").show(), df1.groupBy("children").min("children_feature").show()
  df1.printSchema()
  ```

  ```python
  +------+-------------------+
  |gender|min(gender_feature)|
  +------+-------------------+
  |female|                0.0|
  |  male|                1.0|
  +------+-------------------+
  
  +--------+---------------------+
  |children|min(children_feature)|
  +--------+---------------------+
  |      no|                  1.0|
  |     yes|                  0.0|
  +--------+---------------------+
  root
   |-- affairs: long (nullable = true)
   |-- gender: string (nullable = true)
   |-- age: long (nullable = true)
   |-- label: double (nullable = true)
   |-- children: string (nullable = true)
   |-- religiousness: long (nullable = true)
   |-- education: long (nullable = true)
   |-- occupation: long (nullable = true)
   |-- rating: long (nullable = true)
   |-- gender_feature: double (nullable = false)
   |-- gender_value: vector (nullable = true)
   |-- children_feature: double (nullable = false)
   |-- children_value: vector (nullable = true)
  ```

- 特征提取并转换为指定数据输入格式

  ```python
  from pyspark.ml.feature import VectorAssembler
  
  # 特征选取：affairs为目标值，其余为特征值, 字符
  df2 = df1.select("affairs","age", "religiousness", "education", "occupation", "rating", "gender_value", "children_value")
  
  # 用于计算特征向量的字段
  colArray2 = ["age", "religiousness", "education", "occupation", "rating", "gender_value", "children_value"]
  
  # 如果特征中存在onehot编码，对编码规则执行相对应的onehot编码
  df3 = VectorAssembler().setInputCols(colArray2).setOutputCol("features").transform(df2)
  
  df3.show(5, truncate=False)
  ```

  ```python
  +-------+---+-------------+---------+----------+------+-------------+--------------+---------------------------------------+
  |affairs|age|religiousness|education|occupation|rating|gender_value |children_value|features                               |
  +-------+---+-------------+---------+----------+------+-------------+--------------+---------------------------------------+
  |0      |37 |3            |18       |7         |4     |(2,[1],[1.0])|(2,[1],[1.0]) |[37.0,3.0,18.0,7.0,4.0,0.0,1.0,0.0,1.0]|
  |0      |27 |4            |14       |6         |4     |(2,[0],[1.0])|(2,[1],[1.0]) |[27.0,4.0,14.0,6.0,4.0,1.0,0.0,0.0,1.0]|
  |0      |32 |1            |12       |1         |4     |(2,[0],[1.0])|(2,[0],[1.0]) |[32.0,1.0,12.0,1.0,4.0,1.0,0.0,1.0,0.0]|
  |0      |57 |5            |18       |6         |5     |(2,[1],[1.0])|(2,[0],[1.0]) |[57.0,5.0,18.0,6.0,5.0,0.0,1.0,1.0,0.0]|
  |0      |22 |2            |17       |6         |3     |(2,[1],[1.0])|(2,[1],[1.0]) |[22.0,2.0,17.0,6.0,3.0,0.0,1.0,0.0,1.0]|
  +-------+---+-------------+---------+----------+------+-------------+--------------+---------------------------------------+
  only showing top 5 rows
  ```

### 建模

- 训练

  ```python
  ## 切分训练集和测试
  train_set, test_set = df3.randomSplit([0.8, 0.2], seed=1)
  
  ## 逻辑回归训练
  from pyspark.ml.classification import LogisticRegression
  
  lr = LogisticRegression(labelCol="affairs", featuresCol="features", maxIter=1000)
  model = lr.fit(train_set)
  model.transform(test_set).show(5)
  ```

  ```python
  +-------+--------------------+----------+
  |affairs|         probability|prediction|
  +-------+--------------------+----------+
  |      0|[2.15816512567514...|       1.0|
  |      0|[4.84340472354529...|       1.0|
  |      1|[1.0,1.7712707122...|       0.0|
  |      1|[0.00426898146491...|       1.0|
  |      1|[1.0,3.8410592271...|       0.0|
  +-------+--------------------+----------+
  ```

- 参数解析

  ```python
  # 权重与偏差项
  model.intercept, model.coefficients
  ```

  ```
  (69.02565338517645,
   DenseVector([-6.9903, -40.4182, -18.8954, -12.7829, -8.2078, 110.0572, 160.5141, 520.0981, -245.3642]))
  ```

- 模型保存与加载

  ``` python
  #  保存
  SAVE_PATH = "data/LR_test.obj"
  
  if not os.path.exists(SAVE_PATH):
      model.save(SAVE_PATH)
  else:
      print("模型已保存!")
      
  #  加载模型
  model1 = LogisticRegressionModel.load("data/LR_test.obj")
  ```

