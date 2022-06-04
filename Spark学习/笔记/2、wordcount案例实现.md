# wordcount案例实现

## 实现步骤

读取数据、对数据切片、转换为二元元组、分组累加、输出结果

## 代码实现

### 程序入口

main +tab

### 导入库

from pyspark import ...

### 实例化对象

```py
conf = SparkConf().setMaster().setAppName()
sc = SparkContext(conf=conf)
```

### 读取数据

```py
rdd_init = sc.textFile('../data/words.text')
#print(rdd_init.collect)
```

### 对每一行数据进行切割操作

```py
#得到一个列表结果集（列表里面每个元素是还是一个列表，这样数据结果比较胖）
#rdd_map = rdd_init.map(lambda line: line.split())
#扁平化操作，得到一个列表结果集（每个元素变成一个字符串）
rdd_flatmap = rdd_init.flatMap(lambda line: line.split())
```

### 将字符串元素转换为二元元组

```py
rdd_map = rdd_flatmap.map(lambda word:(word,1))
```

### 根据key分组聚合统计

```py
rdd_res = rdd_map.reduceByKey(lambda agg,curr: agg+curr)
```

### 输出结果

```py
print(rdd_res.collect())
```

## 解决java环境配置问题

#### 修改虚拟机中.bashrc文件

vim ~/.bashrc

在文件中添加以下两行内容：

```shell
export JAVA_HOME=/export/server/jdk1.8.0_241/

export PYSPARK_PYTHON=/root/anaconda3/bin/python3
```



#### 重新加载bashrc

source ~/.bashrc

#### 在代码开头添加以下内容，用于锁定远程版本

```py
os.environ['SPARK_HOME'] = '/export/server/spark'

os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'

os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'
```





## rdd_map.reduceByKey()函数原理

![1653299934106](C:\Users\dasaifudoufu\AppData\Roaming\Typora\typora-user-images\1653299934106.png)



进入base环境:

​	conda activate base

启动anaconda：

​	anaconda-navigator

​	

## pyspark模板





