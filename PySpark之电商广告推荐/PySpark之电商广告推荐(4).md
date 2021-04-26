## 离线推荐处理

**目的**:这里主要是利用我们前面训练的ALS模型进行协同过滤召回，但是注意，我们ALS模型召回的是用户最感兴趣的类别，而我们需要的是用户可能感兴趣的广告的集合，因此**我们还需要根据召回的类别匹配出对应的广告**。 所以,这里我们除了需要我们训练的ALS模型以外，还需要有一个广告和类别的对应关系。

### 整体流程

- behavior_log.csv ==> 评分数据 ==> user-cate/brand评分数据 ==> 协同过滤 ==> top-N cate/brand ==> 关联广告
- 协同过滤召回 ==> top-N cate/brand ==> 关联对应的广告完成召回

### 构建广告与商品类别表

```python
# 加载广告基本信息数据，
df = spark.read.csv("data/ad_feature.csv", header=True)

# 注意：由于本数据集中存在NULL字样的数据，无法直接设置schema，只能先将NULL类型的数据处理掉，然后进行类型转换
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

# 替换掉NULL字符串，替换掉
df = df.replace("NULL", "-1")

# 更改df表结构：更改列类型和列名称
ad_feature_df = df.\
    withColumn("adgroup_id", df.adgroup_id.cast(IntegerType())).withColumnRenamed("adgroup_id", "adgroupId").\
    withColumn("cate_id", df.cate_id.cast(IntegerType())).withColumnRenamed("cate_id", "cateId").\
    withColumn("campaign_id", df.campaign_id.cast(IntegerType())).withColumnRenamed("campaign_id", "campaignId").\
    withColumn("customer", df.customer.cast(IntegerType())).withColumnRenamed("customer", "customerId").\
    withColumn("brand", df.brand.cast(IntegerType())).withColumnRenamed("brand", "brandId").\
    withColumn("price", df.price.cast(FloatType()))

# 这里我们只需要adgroupId、和cateId
_ = ad_feature_df.select("adgroupId", "cateId")
# 由于这里数据集其实很少，所以我们再直接转成 Pandas dataframe来处理，把数据载入内存
pdf = _.toPandas()
pdf.head()
```

|      | adgroupId | cateId |
| :--: | :-------: | :----: |
|  0   |   63133   |  6406  |
|  1   |  313401   |  6406  |
|  2   |  248909   |  392   |
|  3   |  208458   |  392   |
|  4   |  110847   |  7211  |

### 推荐广告

```python
from pyspark.ml.recommendation import ALSModel

als_model = ALSModel.load("data/ALS_Cate_model.obj/")
print("推荐类型：",  als_model.getItemCol(), als_model.getUserCol())

# 获得推荐广告的商品类型
cateId_df = pd.DataFrame(pdf.cateId.unique(), columns=["cateId"])
cateId_df.insert(0, "userId", np.array([427 for i in range(len(cateId_df))]))
result = als_model.transform(spark.createDataFrame(cateId_df))  # 推荐
result.sort("prediction", ascending=False).dropna().show()
```

```python
推荐类型：('cateId', 'userId')
+------+------+----------+
|userId|cateId|prediction|
+------+------+----------+
|   427|  6978|0.31386238|
|   427|  7422|0.28090417|
|   427|  4433|0.20575814|
|   427|  3015|0.19531126|
|   427|  5358|0.19412701|
|   427| 11752|0.19253506|
|   427|  4589|0.18606065|
|   427|   820|0.18226622|
|   427|  5375|0.17778711|
|   427|   474|0.17610353|
|   427|  6294|0.17403731|
|   427|  4815| 0.1676827|
|   427|  5751|0.16453266|
|   427|  4798|0.16450714|
|   427| 11486|0.16103375|
|   427|  8799|0.16067055|
|   427|  4432|0.15562129|
|   427|     7|0.15313935|
|   427|  5523|0.15289858|
|   427|  9383|0.15233861|
+------+------+----------+
only showing top 20 rows
```

```python
# 用户隐含特征
als_model.userFactors.show(10, truncate=False)
```

```
+----+--------------------------------------------------------------------------------------------------------------------------------------+
|id  |features                                                                                                                              |
+----+--------------------------------------------------------------------------------------------------------------------------------------+
|427 |[-0.08862011, 0.024677375, 0.12652709, 0.03105228, -0.015887666, -0.076577045, -0.10301663, 0.020497091, 0.014156798, -0.093687914]   |
|437 |[0.08121957, -0.07566206, -0.08685034, -0.16820733, -0.21639597, 0.10768882, -0.13673659, -0.09585375, 0.082970485, 0.14766619]       |
|747 |[0.031304367, 0.09682198, 0.1347598, 0.034188993, 0.13986087, 0.12803291, -0.16679932, -0.104109496, 0.13648869, 0.23043962]          |
|1247|[0.22964337, -0.26408425, 1.6565194, -0.19833353, 0.52432746, 0.08974882, 0.33008733, 1.3145455, -0.34737256, 0.54083633]             |
|2177|[-0.006012235, 0.11970336, 0.21558432, -0.15280657, 0.0086795185, -0.067469634, -0.038556375, 0.008515552, -0.07740968, 0.05015077]   |
|2247|[-0.06761185, -0.40897173, -0.14313649, 0.20925727, 0.028727109, -0.113159336, 0.15228641, -0.058656577, 0.12894748, -0.011355329]    |
|2527|[0.08582949, -0.07457629, -0.06486175, 0.02869886, -0.06862812, 0.029123373, -0.07809314, 0.018493466, 0.06554801, 0.01668032]        |
|2827|[1.377811, -0.69850916, 2.0206037, -0.5936584, -0.69637454, -0.030831939, 1.3373528, 0.076208815, -0.36765805, 0.19733378]            |
|2877|[-0.05168787, 0.056770574, -0.0032576998, -0.04482137, -0.046533227, -0.014454543, -0.1390726, 0.11489638, -0.096733324, -0.038567316]|
|2947|[0.75573343, -0.43991256, 0.15535285, -0.13316123, 0.656812, -0.10289777, -0.29343438, 0.47792152, -0.069517024, -0.66402215]         |
+----+--------------------------------------------------------------------------------------------------------------------------------------+
only showing top 10 rows
```

```python
# 所有用户推荐

for r in als_model.userFactors.select("id").collect():
    
    userId = r.id
    cateId_df = pd.DataFrame(pdf.cateId.unique(),columns=["cateId"])
    cateId_df.insert(0, "userId", np.array([userId for i in range(len(cateId_df))]))
    
    ret = set()
    # 对用户进行商品推荐,并对预测概率统计
    cateId_list = als_model.transform(spark.createDataFrame(cateId_df)).sort("prediction", ascending=False).dropna()
    
    # 候选20个类别商品进行候选广告推荐
    for i in cateId_list.head(20):
        need = 500 - len(ret)  # 计算是否足够
        ret = ret.union(np.random.choice(pdf.where(pdf.cateId == i.cateId).adgroupId.dropna().astype(np.int64), need))
    # 此处只是测试，并未完整计算出对所有用户的推荐广告id
        if len(ret) >= 500:
            break
        break
    break
    
(userId, *ret)[:10]
```

```python
(427, 2051, 367108, 101892, 18950, 716295, 63502, 12309, 12310, 2074)
```

### 参考

[推荐系统](https://www.jiqizhixin.com/graph/technologies/6ca1ea2d-6bca-45b7-9c93-725d288739c3)

[黑马python5.0](http://www.itheima.com/special/pythonzly/)

[推荐系统（一）：个性化电商广告推荐系统介绍、数据集介绍、项目效果展示、项目实现分析、点击率预测(CTR--Click-Through-Rate)概念](https://blog.csdn.net/qq_35456045/article/details/104881691?ops_request_misc=%257B%2522request%255Fid%2522%253A%2522159935430019724839860552%2522%252C%2522scm%2522%253A%252220140713.130102334.pc%255Fall.%2522%257D&request_id=159935430019724839860552&biz_id=0&utm_medium=distribute.pc_search_result.none-task-blog-2~all~first_rank_ecpm_v3~rank_business_v1-6-104881691.ecpm_v3_rank_business_v1&utm_term=%E4%B8%AA%E6%80%A7%E5%8C%96%E7%94%B5%E5%95%86%E6%8E%A8%E8%8D%90&spm=1018.2118.3001.4187)