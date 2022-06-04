# Spark SQL的数据写出操作

## 统一写出的API:

![1654084741610](C:\Users\dasaifudoufu\AppData\Roaming\Typora\typora-user-images\1654084741610.png)

```properties
上述的完整的API 同样也有简单写法:
	df.write.mode().输出类型()
	
	比如说:
		df.write.mode().csv()
		df.write.mode().json()
		....
```

## 代码演示:

### 写出到文件

```python
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("演示数据写出到文件中")

    # 1- 创建SparkSession对象:
    spark = SparkSession.builder.master('local[*]').appName('movie').config("spark.sql.shuffle.partitions", "1").getOrCreate()

    # 2- 读取HDFS上movie数据集
    df_init = spark.read.csv(
        path='file:///export/data/workspace/sz30_pyspark_parent/_03_pyspark_sql/data/stu.csv',
        sep=',',
        header=True,
        inferSchema=True
    )
    # 3- 对数据进行清洗操作:
    # 演示一:  去重API:  df.dropDuplicates()
    df = df_init.dropDuplicates()
    # 演示二: 2-  删除null值数据:  df.dropna()
    df = df.dropna()
	
    # 4- 将清洗后的数据写出到文件中
    # 演示写出为CSV文件
    #df.write.mode('overwrite').format('csv').option('header',True).option('sep','|').save('hdfs://node1:8020/sparkwrite/output1')

    # 演示写出为 JSON
    #df.write.mode('overwrite').format('json').save('hdfs://node1:8020/sparkwrite/output2')

    # 演示输出为text
    #df.select('name').write.mode('overwrite').format('text').save('hdfs://node1:8020/sparkwrite/output3')

    # 演示输出为orc
    df.write.mode('overwrite').format('orc').save('hdfs://node1:8020/sparkwrite/output4')

```

### 写出到HIVE表

```python
df.write.mode('append|overwrite|ignore|error').saveAsTable('表名','存储类型')

# 说明: 目前无法演示输出到HIVE , 因为 Spark 和 HIVE没有整合
```

写出到MYSQL表

```properties
df.write.mode('append|overwrite|ignore|error').format('jdbc')
.option("url","jdbc:mysql://xxx:3306/库名?useSSL=false&useUnicode=true&characterEncoding=utf-8")\
.option("dbtable","表名")\
.option("user","用户名")\
.option("password","密码")\
.save()

说明:
	当表不存在的时候, 会自动建表, 对于overwrite来说, 每次都是将表删除, 重建
	
	如果想要自定义字段的类型, 请先创建表, 然后使用append的方式来添加数据即可
```

代码演示

```python
#mysql建库
CREATE DATABASE  day06_pyspark CHARSET utf8;
#写入
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("演示数据写出到文件中")

    # 1- 创建SparkSession对象:
    spark = SparkSession.builder.master('local[*]').appName('movie').config("spark.sql.shuffle.partitions", "1").getOrCreate()

    # 2- 读取HDFS上movie数据集
    df_init = spark.read.csv(
        path='file:///export/data/workspace/sz30_pyspark_parent/_03_pyspark_sql/data/stu.csv',
        sep=',',
        header=True,
        inferSchema=True
    )
    # 3- 对数据进行清洗操作:
    # 演示一:  去重API:  df.dropDuplicates()
    df = df_init.dropDuplicates()
    # 演示二: 2-  删除null值数据:  df.dropna()
    df = df.dropna()

    # 4- 将清洗后的数据写出到文件中
    
    # 演示输出MySQL
    df.write.mode('overwrite').format('jdbc')\
        .option("url", "jdbc:mysql://node1:3306/day06_pyspark?useSSL=false&useUnicode=true&characterEncoding=utf-8") \
        .option("dbtable", "stu") \
        .option("user", "root") \
        .option("password", "123456") \
        .save()
```

极有可能遇到的一个错误

![1654085406043](C:\Users\dasaifudoufu\AppData\Roaming\Typora\typora-user-images\1654085406043.png)

```
原因:
	当前Spark无法找到一个适合的驱动连接MySQL

解决方案: 添加MySQL的驱动包, 需要在以下几个位置中添加驱动包
	1- 在python的环境中添加mysql的驱动包 (在pycharm本地右键运行的时候, 需要加载)
		Base的pyspark库的相关的jar包路径:  /root/anaconda3/lib/python3.8/site-packages/pyspark/jars/
		虚拟环境: /root/anaconda3/envs/虚拟环境名称/lib/python3.8/site-packages/pyspark/jars/

	2- 需要在 Spark的家目录下jars目录下添加mysql的驱动包 (spark-submit提交到spark集群或者local模式需要使用)
		/export/server/spark/jars/
	
	3- 需要在HDFS的/spark/jars目录下添加mysql的驱动包 (spark-submit提交到yarn环境的时候)


建议: 如果是常用的jar包, 建议以上三个位置都添加
```

