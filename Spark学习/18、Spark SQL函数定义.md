# Spark SQL函数定义

## 如何使用窗口函数

### 回顾窗口函数:

```properties
窗口函数格式: 
	分析函数 over(partition by xxx order by xxx [asc | desc] [rows between  窗口范围 and 窗口范围 ])
	
分析函数:
	第一类函数:  row_number() rank() dense_rank() ntile(N)
	
	第二类函数: 与聚合函数组合使用  sum() max() min() avg() count()

	第三类函数:  lead() lag() frist_value() last_value()
```

### 这些窗口函数如何在spark SQL中使用呢?

#### 通过SQL的方式 : 与在hive中使用基本是雷同	

```properties
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("演示spark SQL的窗口函数使用")

    # 1- 创建SparkSession对象:
    spark = SparkSession.builder.master('local[*]').appName('windows').getOrCreate()

    # 2- 读取外部数据源
    df_init = spark.read.csv(path='file:///export/data/workspace/sz30_pyspark_parent/_03_pyspark_sql/data/pv.csv',header=True,inferSchema=True)

    # 3- 处理数据操作:  演示窗口函数
    df_init.createTempView('pv_t1')
    # 3.1  SQL方式
    spark.sql("""
        select
            uid,
            datestr,
            pv,
            row_number() over(partition by uid order by pv desc) as rank1,
            rank() over(partition by uid order by pv desc) as rank2,
            dense_rank() over(partition by uid order by pv desc) as rank3
        from pv_t1
    """).show()
```

#### DSL方式

```sql
	df_init.select(
        df_init['uid'],
        df_init['datestr'],
        df_init['pv'],
       	F.row_number().over(win.partitionBy('uid').orderBy(F.desc('pv'))).alias('rank1'),
        F.rank().over(win.partitionBy('uid').orderBy(F.desc('pv'))).alias('rank_2'),
        F.dense_rank().over(win.partitionBy('uid').orderBy(F.desc('pv'))).alias('rank_3')
	).show()
```

## SQL函数的分类说明

### 回顾 SQL 函数的分类

#### UDF:   用户自定义函数

​	特点: 一进一出  大部分的内置函数都是属于UDF函数  

​	比如  substr() 

#### UDAF:  用户自定义聚合函数

​	特点: 多进一出 

​	比如: sum()  count() avg()....

#### UDTF: 用户自定义表生成函数

​	特点:  一进多出  (给一个数据, 返回多行或者多列的数据)

​	比如: explode()  爆炸函数,json_tuple,pass_url等

总结:其实不管是spark SQL中  内置函数, 还是hive中内置函数, 其实也都是属于这三类中其中一类

### 自定义函数目的

​	扩充函数, 因为在进行业务分析的时候, 有时候会使用某些功能, 但是内置函数并没有提供这样的功能, 此时需要进行自定义函数来解决

```properties
	注意①:对于spark SQL目前支持定义 UDF 和 UDAF , 但是对于python语言 仅支持定义UDF函数, 如果想要定义UDAF函数, 需要使用pandas UDF实现
	注意②:	在使用python 原生 spark SQL的 UDF方案, 整个执行效率不是特别高, 因为整个内部运算是一个来处理., 一个个返回, 这样会导致频繁进行 序列化和反序列化的操作 从而影响效率
	后续改进版本: 采用java 或者scala来定义, 然后python调用即可
	
	目前主要使用版本: 是采用pandas的UDF函数解决, 同时采用apache arrow 内存数据结构框架 来实现, 提升整个执行效率
```

### Spark原生自定义UDF函数

#### 如何自定义UDF函数

```properties
1- 第一步: 根据业务功能要求,  定义一个普通的Python的函数
2- 第二步: 将这个python的函数注册到Spark SQL中(声明这个普通函数作为Spark SQL的UDF函数)
	注册方式有以下二种方案:
		方式一:  可适用于  SQL 和 DSL
			udf对象 = sparkSession.udf.register(参数1, 参数2,参数3)
			
			参数1: udf函数的函数名称, 此名称用于在SQL风格中使用
			参数2: 需要将哪个python函数注册为udf函数
			参数3: 设置python函数返回的类型
			
			udf对象 主要使用在DSL中
			
		方式二: 仅适用于 DSL方案
			udf对象 =F.udf(参数1,参数2)
			
			参数1: 需要将哪个python函数注册为udf函数
			参数2: 设置python函数返回的类型
			udf对象 主要使用在DSL中
		方式二还有一种语法糖写法:  @F.udf(设置返回值类型)  底层走的是装饰器
			放置在普通的python函数的上面
3- 在SQL或者 DSL中使用即可
```

#### 自定义UDF函数演示操作

```python
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
import pyspark.sql.functions as F
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("演示如何自定义UDF函数")

    # 1- 创建SparkSession对象
    spark = SparkSession.builder.master('local[*]').appName('udf_01').getOrCreate()

    # 2- 读取外部的数据集
    df_init = spark.read.csv(
        path='file:///export/data/workspace/sz30_pyspark_parent/_03_pyspark_sql/data/student.csv',
        schema='id int,name string,age int'
    )
    df_init.createTempView('t1')
    # 3- 处理数据
    # 需求: 自定义函数需求  请在name名称后面 添加一个  _itcast
    # 3.1 自定义一个python函数, 完成主题功能
    # 方式三: 不能和方式二共用
    # @F.udf(returnType=StringType())
    def concat_udf(name: str) -> str:
        return name + '_itcast'
    
    # 3.2 将函数注册给spark SQL
    #方式一:
    concat_udf_dsl = SparkSession.udf.register('concat_udf_sql',concat_udf,StringType())
    
    #方式二:
    concat_udf_dsl_2 = F.udf(concat_udf,StringType())
    
    # 3.3 使用自定义函数
    # SQL使用
    spark.sql("""
    	select 
    		id,
    		concat_udf_sql(name) as name,
    		age
    	from t1
    """).show()
    #DSL使用
    df_init.select(
    	'id',
        concat_udf_dsl(df_init['name']).alias('name'),
        'age'
    ).show()
    df_init.select(
        df_init['id'],
        concat_udf_dsl_2(df_init['name']).alias('name'),
        df_init['age']
    ).show()

    df_init.select(
        df_init['id'],
        concat_udf(df_init['name']).alias('name'),
        df_init['age']
    ).show()
```

演示返回类型为 字典/列表

```properties
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import  *
import pyspark.sql.functions as F
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("演示spark sql的UDF函数: 返回列表/字典")
    # 1- 创建SparkSession对象
    spark = SparkSession.builder.master('local[*]').appName('udf_01').getOrCreate()

    # 2- 读取外部的数据集
    df_init = spark.read.csv(
        path='file:///export/data/workspace/sz30_pyspark_parent/_03_pyspark_sql/data/user.csv',
        header=True,
        inferSchema=True
    )
    df_init.createTempView('t1')

    # 3- 处理数据
    # 需求: 自定义函数 请将line字段切割开, 将其转换 姓名 地址 年龄
    # 3.1 定义一个普通python的函数
    def split_3col_1(line):
        return line.split('|')
        
    def split_3col_2(line):
        arr = line.split('|')
        return {'name':arr[0],'address':arr[1],'age':arr[2]}
        
   	# 3.2 注册
    # 对于返回列表的注册方式
    # 方式一
    schema = StructType().add('name',StringType()).add('address',StringType()).add('age',StringType())
    split_3col_1_dsl = spark.udf.register('split_3col_1_sql',split_3col_1,schema)
    
     # 对于返回为字典的方式
    # 方式二:
    schema = StructType().add('name', StringType()).add('address', StringType()).add('age', StringType())
    split_3col_2_dsl = F.udf(split_3col_2,schema)
    
     # 3.3 使用自定义函数
    spark.sql("""
        select
            userid,
            split_3col_1_sql(line) as 3col,
            split_3col_1_sql(line)['name'] as name,
            split_3col_1_sql(line)['address'] as address,
            split_3col_1_sql(line)['age'] as age
        from t1
    """).show()
    
     # DSL中
    df_init.select(
        'userid',
        split_3col_2_dsl('line').alias('3col'),
        split_3col_2_dsl('line')['name'].alias('name'),
        split_3col_2_dsl('line')['address'].alias('address'),
        split_3col_2_dsl('line')['age'].alias('age')
    ).show()
```

### 基于pandas的Spark UDF函数

####  apache arrow基本介绍

​	apache arrow 是apache旗下的一款顶级的项目, 是一个跨平台的在内存中以列式存储的数据层, 它设计的目的是作为一个跨平台的数据层, 来加快大数据分析项目的运行效率

​	pandas与pyspark SQL 进行交互的时候, 建立在apache arrow上, 带来低开销 高性能的UDF函数

​	arrow 并不会自动使用, 需要对配置以及代码做一定小的更改才可以使用并兼容

如何安装?

```properties
	pip install pyspark[sql]
	
	说明: 三个节点要求要安装, 如果使用除base虚拟环境以外的环境, 需要先切换到对应虚拟环境下
	
	注意: 
		如果安装比较慢, 可以添加一下 清华镜像源
			pip install -i https://pypi.tuna.tsinghua.edu.cn/simple pyspark[sql]
			
		不管是否使用我的虚拟机, 都建议跑一次, 看一下是否存在
```

如何使用呢?

```properties
	spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
```

![1654089398285](C:\Users\dasaifudoufu\AppData\Roaming\Typora\typora-user-images\1654089398285.png)

### 如何基于arrow完成pandas DF 与 Spark DF的互转操作

​	如何将pandas的DF对象 转换 spark的DF , 以及如何从spark df 转换为 pandas的 df对象

```properties
pandas DF  --> Spark DF : 
	spark_df = sparkSession.createDataFrame(pd_df)
	
Spark DF  ---> pandas DF:
	pd_df = spark_df.toPandas()
```

代码演示

```properties
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pandas as pd
import pyspark.sql.functions as F
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("pySpark模板")

    # 1- 创建SparkSession对象
    spark = SparkSession.builder.master('local[*]').appName('udf_01').getOrCreate()

    # 开启arrow
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

    # 2- 创建 pandas的DF对象:
    pd_df = pd.DataFrame({'name':['张三','李四','王五'],'age':[20,18,15]})
    
    # 3- 可以使用panda的API来对数据进行处理操作
    pd_df = pd_df[pd_df['age'] > 16]
    print(pd_df)

    # 4- 将其转换为Spark DF
    spark_df = spark.createDataFrame(pd_df)


    # 5- 可以使用spark的相关API处理数据
    spark_df = spark_df.select(F.sum('age').alias('sum_age'))

    spark_df.printSchema()
    spark_df.show()

    # 6- 还可以将spark df 转换为pandas df
    pd_df = spark_df.toPandas()

    print(pd_df)
```

请注意: 开启arrow方案, 必须先安装arrow, 否则无法使用, 一执行就会报出: no module 'pyarrow'  (没有此模块)

```properties
pandas UDF代码, 请各位先检查 当前虚拟机中pyspark库是否为3.1.2

命令:
	conda list | grep pyspark
```

### pandas UDF

- 方式一: series TO  series
  - 描述: 定义一个python的函数, 接收series类型, 返回series类型, 接收一列返回一列 
  - 目的: 用于定义 pandas的UDF函数

![1654089872491](C:\Users\dasaifudoufu\AppData\Roaming\Typora\typora-user-images\1654089872491.png)

代码演示:

```properties
import pandas as pd
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("基于pandas定义UDF函数")
    # 1- 创建SparkSession对象
    spark = SparkSession.builder.master('local[*]').appName('udf_01').getOrCreate()

    # 2- 构建数据集
    df_init = spark.createDataFrame([(1,3),(2,5),(3,8),(5,4),(6,7)],schema='a int,b int')
    df_init.createTempView('t1')
    # 3- 处理数据:
    # 需求: 基于pandas的UDF 完成 对 a 和 b列乘积计算
    # 3.1 自定义一个python的函数: 传入series类型, 返回series类型
    @F.pandas_udf(returnType=IntegerType())  # 装饰器  用于将pandas的函数转换为 spark SQL的函数
    def pd_cj(a:pd.Series,b:pd.Series) -> pd.Series:
        return a * b

    #3.2 对函数进行注册操作
    # 方式一:
    pd_cj_dsl = spark.udf.register('pd_cj_sql',pd_cj)

    #3.3 使用自定义函数
    # SQL
    spark.sql("""
        select a,b, pd_cj_sql(a,b) as cj from  t1
    """).show()
    
    # DSL
    df_init.select('a','b',pd_cj('a','b').alias('cj')).show()
```

- 从series类型 到 标量(python基本数据类型) :
  - 描述: 定义一个python函数, 接收series类型的数据, 输出为标量, 用于定义 UDAF函数

```properties
import pandas as pd
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("基于pandas定义UDF函数")
    # 1- 创建SparkSession对象
    spark = SparkSession.builder.master('local[*]').appName('udf_01').getOrCreate()

    # 2- 构建数据集
    df_init = spark.createDataFrame([(1,3),(1,5),(1,8),(2,4),(2,7)],schema='a int,b int')
    df_init.createTempView('t1')
    # 3- 处理数据:
    # 需求: pandas的UDAF需求   对 B列 求和
    # 3.1 创建python的函数:  接收series类型, 输出基本数据类型(标量)
    @F.pandas_udf(returnType=IntegerType())
    def pd_b_sum(b:pd.Series) -> int:
        return b.sum()


    # 3.2 注册函数
    spark.udf.register('pd_b_sum_sql',pd_b_sum)

    # 3.3 使用自定义函数
    spark.sql("""
        select
            pd_b_sum_sql(b) as avg_b
        from t1
    """).show()

    spark.sql("""
            select
                a,
                b,
                pd_b_sum_sql(b) over(partition by a order by b) as sum_b
            from t1
        """).show()
```

