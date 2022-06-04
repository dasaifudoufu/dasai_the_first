# 构建RDD的两种方式

## 方式一：通过并行化本地集合的方式

```python
# 演示如何创建RDD对象" 方式一
from pyspark import SparkContext, SparkConf
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("演示:如何创建RDD")
    # 1- 构建SparkContext对象
    conf = SparkConf().setMaster("local[3]").setAppName('create_rdd')
    sc = SparkContext(conf=conf)

    # 2- 构建RDD对象
    rdd_init = sc.parallelize(['张三','李四','王五','赵六','田七','周八','李九'],5)

    # 需求:给每一个元素添加一个 10序号
    rdd_map = rdd_init.map(lambda name: name + '10')

    # 如何查看RDD中每个分区的数据,以及分区的数量
    print(rdd_init.getNumPartitions())
    print(rdd_init.glom().collect())

    print(rdd_map.glom().collect())
```



## 方式二：通过引用加载外部存储系统的方式构建

```sql
from pyspark import SparkContext,SparkConf
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'
	
if __name__ == '__main__':

	1-创建SparkContext上下文对象
	conf = SparkConf.().setMaster().setAppName('create_add_2')
	sc = SparkContext(conf=conf)
	
	2-读取外部文件数据
	#路径写法：协议 + 路径
	#本地：file://
	#外部读取:hdfs://node1:8020/
	rdd_init = sc.textFile('hdfs://node1:8020/pyspark_data/words.txt')
	#合并小文件的算子wholeTextFile
	rdd_init = sc.wholeTextFile('hdfs://node1:8020/pyspark_data/words.txt')
	print(rdd_init.getNumPartitions())
	print(rdd_init.glom().collect())
```

## 总结问题：RDD分区数

### RDD分区的重要意义：

​	数据在RDD内部被切分为多个子集合，每个子集合可以被认为是一个分区，运算逻辑最小会被应用在每一个分区上，每个分区是由一个单独的任务（task）来运行的，所以分区数越多，整个应用的并行度会越高

### 取决因素：

​	第一点：RDD分区的原则是使得分区的个数尽量等于集群中的CPU核心（core）数目，这样可以充分利用CPU的计算资源（虚拟机分配的核数）

​	第二点：在实际中为了更加充分的压榨CPU的计算资源，会把并行度设置为cpu核数的2~3倍

​	第三点：RDD分区数和启动时指定的核数、调用方法时指定的分区数、文件本身分区数有关系，具体如下：

1、

​	



# hdfs小文件过多问题

产生大量的元数据文件，而元数据存储在集群的namenode节点内存中。过多的元数据文件会占用namenode内存，导致虽然datanode磁盘仍有大量存储，但是受限于namenode内存占用，无法存储新的文件。所以需要细心维护数据的元数据，合理规划，比如小文件合并处理。