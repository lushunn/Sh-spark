# -*- coding: utf-8 -*-
"""
Created on Wed Jun 19 09:34:00 2019

@author: Dell
"""


import pyspark
from pyspark.sql import Row

### START CODE HERE ### (<5 lines of code)
'''
读入iptv_equipment.txt文件
read the txt file 

tips :the functions you maybe need 
#sc.textFile()
#pyspark.rdd.RDD.map()
#sqlContext.createDataFrame()

input 
   path:path of iptv_equipment.txt

output
     df:pyspark.sql.dataframe.DataFrame

'''
### END CODE HERE ###

#提示:在 spark 2.0以上版本，可以直接通过spark.read.csv载入为datafame

df.show()

#重命名列名
c = ['Equment_id', 'bras_olt_ip', 'Equment_name', 'area_code', 'higher_eq_id', 'higher_eq_name', 'Unit_type', 'record_date', 'count_time_type', 'play_users', 'good_num', 'over_lagcountAndTs_num', 'over_loss_num', 'over_lag_num', 'swtime_num', 'good_swtime_num', 'live_swtime_num', 'good_live_swtime_num', 'ts_num']
df=df.toDF(*c)

#查看字段类型 
df.dtypes

#将原始数据中缺失值"-1"标记为空值 None
from pyspark.sql.functions import udf
##UDF函数

new_column_udf = udf(lambda name: None if (name == "-1"  or  name == "NULL" )else name)
### START CODE HERE ### (<3 lines of code)
'''
利用UDF函数标记缺失值
apply the udf to every columns of dataframe
tips :the functions you maybe need 
#pyspark.sql.dataframe.DataFrame.withColumn()

input 
   df:pyspark.sql.dataframe.DataFrame

output
     df:pyspark.sql.dataframe.DataFrame
'''
### END CODE HERE ###




#查看缺失值情况
from pyspark.sql.functions import isnull, when, count, col
df.select([count(when(isnull(c), c)).alias(c) for c in df.columns]).show()



from  pyspark.sql.types import IntegerType
##转变字段类型
for i in ['record_date','good_num','play_users','over_lagcountAndTs_num',
 'over_loss_num',
 'over_lag_num',
 'swtime_num',
 'good_swtime_num',
 'live_swtime_num',
 'good_live_swtime_num','area_code','higher_eq_id','record_date']:
    df = df.withColumn(i, df[i].cast(IntegerType()))


#清洗掉播放用户数为0的设备数据
import pyspark.sql.functions as f
zero_eq_id = df.filter((f.col('play_users') == 0)).select('Equment_id').distinct()
zero_eq_id = set([row['Equment_id'] for row in zero_eq_id.collect()])
df_filter = df.where(~col('Equment_id').isin(zero_eq_id))



#生成新字段 优良播放率
df_filter = df_filter .withColumn('good_per', df_filter.good_num / df_filter.play_users)

#按照时间排序
### START CODE HERE ### (<2 lines of code)
'''
sort the dataframe by the column 'record_date'
tips :the functions you maybe need 
#sc.textFile
pyspark.sql.dataframe.DataFrame.orderBy()
input 
   df_filter:pyspark.sql.dataframe.DataFrame

output
    df_filter:pyspark.sql.dataframe.DataFrame
'''
### END CODE HERE ###

##生成历史优良率dict，{设备id:[历史播放优良率]}
history_good_per=dict()
def helper(row):
    if row.Equment_id in history_good_per:
         history_good_per[ row.Equment_id ].append(row.good_per)
    else :
         history_good_per[ row.Equment_id ]=[row.good_per]

for row in df_filter.rdd.collect():
    helper(row)

#按时间窗口，分裂成规整数据
win=7
def regular(data, window=win):
     output_data = []
     filterd_data ={}
     for k, v in history_good_per.items():
          if len(v) > window:
               filterd_data[k]=v
     for k, v in filterd_data.items():
          for i in range(len(v)):
               if i + window + 1 <= len(v):
                    output_data.append([k] + v[i:i + window + 1])  #第一列：id名称，其他列依次是滞后n阶的历史优良率
     return output_data

data_f = regular(history_good_per)
data_f = sqlContext.createDataFrame(data_f)

#day1 over

#添加临时索引，关联用
from pyspark.sql.functions import monotonically_increasing_id
data_f=data_f.withColumn('index1', monotonically_increasing_id())

#特征工程，生成交叉特征
df_filter=df_filter.withColumn('over_lagcountAndTs_per', df_filter.over_lagcountAndTs_num/ df_filter.play_users)
df_filter=df_filter.withColumn('over_loss_per', df_filter.over_loss_num/ df_filter.play_users)
df_filter=df_filter.withColumn('over_lag_per', df_filter.over_lag_num/ df_filter.play_users)
df_filter=df_filter.withColumn('good_swtime_per', df_filter.good_swtime_num/ df_filter.swtime_num+0.0001)
df_filter=df_filter.withColumn('good_live_swtime_per', df_filter.good_live_swtime_num/ df_filter.live_swtime_num+0.0001)

#对所有特征字段，按照设备id和时间窗口进行重铸
for i in ['good_num','play_users','over_lagcountAndTs_num',
 'over_loss_num',
 'over_lag_num',
 'swtime_num',
 'good_swtime_num',
 'live_swtime_num',
 'good_live_swtime_num','over_lagcountAndTs_per',
 'over_loss_per','over_lag_per','good_swtime_per','good_live_swtime_per']:
    history_good_per_tmp=dict()
    def helper(row):
        if row.Equment_id in history_good_per_tmp:
             history_good_per_tmp[row.Equment_id].append(row[i])
        else :
             history_good_per_tmp[ row.Equment_id ]=[row[i]]
    for row in df_filter.rdd.collect():
        helper(row)
    def regular(data,window=win):
        output_data=[]
        filter_key=[i['_1'] for i in data_f.select('_1').distinct().collect()]
        filterd_data = {}
        for k, v in history_good_per_tmp.items():
            if k in filter_key:
                 filterd_data[k]=v
        for k,v in filterd_data.items():
            for i in range (len(v)):
                if i+window+1<=len(v):
                    output_data.append([k]+v[i:i+window+1])#第一列：id名称，其他列依次是滞后n阶的历史优良率
        return output_data
    add_feature=regular(history_good_per_tmp)
    add_feature=sqlContext.createDataFrame(add_feature)
    
    add_feature=add_feature.select(add_feature.columns[1:-1])
    c = [i + '_' + str(j + 1) for j in range(win)]
    add_feature = add_feature.toDF(*c)
    add_feature= add_feature.withColumn('index2', monotonically_increasing_id())
    data_f = data_f.join(add_feature, data_f.index1 == add_feature.index2)
    data_f=data_f.select(data_f.columns[:-1])

#拆分训练测试集，训练模型，输出预测结果
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import GradientBoostedTrees
##去除有部分id零时关联字段
c = list(filter(lambda x: 'index' not in x, data_f.columns))
#把预测字段优质播放比放在最后一列
c=c[1:8] + c[9:]+c[8:9]
df = data_f.select(c)
df = df.dropna()

### START CODE HERE ### 
'''
拆分训练测试集，训练模型，输出预测结果
the package you maybe need：
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import GradientBoostedTrees
from pyspark.mllib.evaluation import RegressionMetrics

the funcions you maby need:
DataFrame.randomSplit()
sc.parallelize()
RegressionMetrics(predictionAndObservations).meanAbsoluteError



'''
### END CODE HERE ###