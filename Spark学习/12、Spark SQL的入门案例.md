# Spark SQL的入门案例

## Spark SQL的统一入口

​	从Spark SQL开始，需要将核心对象，从SparkContext切换为Spark Session对象

```properties
Spark Session对象是Spark2.0后推出一个全新的对象, 此对象将会作为Spark整个编码入口对象, 此对象不仅仅可以操作Spark SQL还可以获取到SparkContext对象, 用于操作Spark Core代码
```

## 如何构建一个Spark Session对象

代码演示：

```python
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("演示: 如何构建Spark Session")

    # 1- 创建SparkSession对象:
    spark = SparkSession.builder.master('local[*]').appName('create_park').getOrCreate()

    print(spark)
```



## Spark SQL的入门案例

- 1- 在_03_pyspark_sql项目中的data目录下创建一个stu.csv 文本文件

```properties
文件内容如下:

id,name,age
1,张三,20
2,李四,18
3,王五,22
4,赵六,25
```



- 2- 代码实现:  需求 请将年龄大于20岁的数据获取出来

```python
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("spark SQL 入门案例")
    
    
    # 1- 构建SparkSession对象
    spark = SparkSession\
        .builder\
        .master('local[*]')\
        .appName('init_pyspark SQL')\
        .getOrCreate()
    # 2- 读取外部文件的数据
    df_init = spark.read.csv(
       		 		      	path='file:///export/data/workspace/sz30_pyspark_parent/_03_pyspark_sql/data/stu.csv',
        header=True,	#uses the first line as names of columns
        sep=',',	#it uses the default value, ``,``
        inferSchema=True
    )
     # 3- 获取数据 和 元数据信息
    df_init.show()
    df_init.printSchema()

    # 4- 需求: 将年龄大于20岁的数据获取出来 :  DSL
    df_where = df_init.where('age > 20')
    df_where.show()

    # 使用 SQL来实现
    df_init.createTempView('t1')

    spark.sql("select *  from  t1 where age > 20").show()
    
    # 关闭 spark对象
    spark.stop()
```





