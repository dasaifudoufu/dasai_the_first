# RDD的内核调度

## RDD的依赖

​	RDD之间存在依赖关系，这也是RDD中非常重要的一个特性，一般将RDD之间的依赖关系划分为两种依赖关系：窄依赖和宽依赖

- 窄依赖

```properties
目的:让各个分区的数据可以并行的计算操作

含义:上一个RDD的某一个分区的数据，被下一个RDD的某一个分区全部都继承下来，我们将这种关系称为窄依赖关系

```

- 宽依赖

```properties
目的:划分stage阶段
描述:上一个RDD的分区的数据被下一个RDD的多个分区所接收并处理（shuffle），我们将这种关系称为宽依赖

所以说，判断两个RDD之间是否存在宽依赖，主要看两个RDD之间是否存在shuffle。一旦产生了shuffle，必须是前面的计算完成后，才能进行后续的计算操作
```

说明：

```properties
	在spark中，每一个算子是否存在shuffle操作，在spark设计的时候就已经确定了，比如说Map一定不会有shuffle，reduceByKey一定存在shuffle
	
	如何判断这个算子是否会走shuffle:可以查看DAG执行流程图，如果发现执行到这个算子时，阶段被划分为多个，一定存在shuffle。也可以通过查看每个算子的文档说明信息来确定
	
	注意:虽然shuffle会降低执行的效率，但是实际使用时以需求为导向，该用就用。
	
```



## DAG与stage

DAG:有向无环图

```properties
整个的流程，有方向，不能往回走，不断的往下继续的过程
```

如何形成一个DAG执行流程图

```properties
1-Driver会将所有的RDD基于依赖关系，形成一个stage的流程图

2-对整个RDD从后往前进行回溯，当遇到RDD之间依赖关系为宽依赖的时候，自动分为一个阶段，如果是窄依赖，合并到一个阶段中

3- 当整个回溯全部完成之后，形成了DAG执行流程图
```

深度剖析，内部的处理操作 

```properties
textFile形成RDD的分区数量确定：
	1-分区数量 = 
	2-

经过shuffle后，RDD的分区数量取决于父RDD的最大分区数量，当如果没有父RDD，分区的数量：
	1-如果是本地模式，取决于本地的CPU核心数
	2-如果使用mesos调度引擎，默认为8
	3-对于其他的调度引擎，比如YARN，取决于max（所有执行节点CPU总核数，2）
	
	手动设置并行度：spark.default.parallelism
```

## RDD的shuffle

- 发展历程

```properties
spark中shuffle历史进程:  
	1- 在Spark 1.1以前的版本中, 整个Spark采用shuffle方案为 HASH shuffle
	2- 在Spark 1.1版本的时候, 引入 Sort shuffle,  主要增加合并排序操作, 对原有HASH shuffle 进行优化
	3- 在Spark 1.5 版本的时候, 引入钨丝计划: 优化操作, 提升内存以及CPU运行
	4- 在Spark 1.6版本的时候 将钨丝计划合并到sort Shuffle中
	5- 在spark 2.0版本以后, 删除掉 HASH shuffle, 全部合并到Sort shuffle中
```

​	

优化前的Hash Shuffle:



```properties
shuffle过程:
	父RDD的每个分区（线程）在生产各个分区数据的时候，会产生与子RDD分区数量相等的文件数量，每个文件对应一个子RDD的分区
	当父RDD执行完成后，子RDD从父RDD产生的文件中，找出对应分区文件，直接拉取处理即可
弊端：
	父RDD产出的分区文件数量太多了，从而在HDFS上产生了大量的小文件
	由于文件变多了，对应磁盘IO也增大了，需要打开文件N次
	子RDD拉取数据，文件数量也比较多，磁盘IO比较大，对效率有比较大的影响
```

优化后的shuffle



```properties
经过优化后的HASH SHUFFLE，整个生成的文件数量整体下降很多

	将原来由各个线程来生成N个分区文件，变更为由executor来统一生成与下游RDD分区数量相同的文件数量即可，这样各个线程在输出数据的时候，将对应分区的数据输出到对应分区文件即可，下游的RDD在拉取数据的时候，只需要拉取自己分区文件的数据即可
```

sort shuffle:



```properties
sort shuffle流程：
	首先父RDD的各个流程将数据分区后写入到内存中，当内存达到一定的阈值后，就会触发溢写操作，将数据溢写到磁盘上（分批次溢写：lw），不断输出，不断溢写，产生多个小文件，当整个父RDD的数据处理完后，然后对小文件 进行合并操作，形成一个最终的文件，同时每一个文件都匹配一个索引文件，用于下游的RDD在拉取数据的时候，根据索引文件快速找到相对应的分区数据
	
```

在sort shuffle中有两种机制：普通机制和bypass机制

```properties
普通机制:带有排序操作
	首先父RDD的各个流程将数据分区后写入到内存中，当内存达到一定的阈值后，就会触发溢写操作，将数据溢写到磁盘上（分批次溢写：lw）,在溢写的过程，会对数据进行排序操作，不断输出，不断溢写，产生多个小文件，当整个父RDD的数据处理完后，然后对小文件 进行合并操作，形成一个最终的文件，同时每一个文件都匹配一个索引文件，用于下游的RDD在拉取数据的时候，根据所以呢文件快速找到相对应的分区数据
	
bypass机制：不含排序，并不是所有的sort shuffle都可以走bypass
	满足以下的条件：
		1-上游的RDD分区数量要小于200
		2-上游不能执行提前聚合的操作
执行bypass机制，由于没有了排序的操作，整个执行效率要高于普通机制

排序：是为了后续可以更快速的进行分组聚合操作
```

## JOB调度流程	

   Driver底层调度方案：

```properties
Driver中的核心对象：
	sparkcontext、DAGSchedule、TaskSchedule、scheduleBackend(资源调度平台)


1-当spark应用的时候，首先执行Main函数，创建一个sparkcontext对象，当这个对象创建，底层同时构建DAGSchedule和TaskSchedule
2-当Spark发现后续的代码有action算子后，就会立即触发任务的执行，生成一个JOB任务，一个action就会触发一个Job任务
3-触发任务后，首先由Driver负责任务分配工作（DAG流程图，stage划分，每个stage需要运行多少个线程，每个线程需要在哪个executor上...）
	3.1-首先由Driver中DAGSchedule执行，主要进行DAG流程图的生成，以及划分stage，并且还会划分出每个stage阶段需要运行多少个线程，并将每个阶段的线程封装到一个TaskSet的列表中，有多少个阶段，就会产生多少个TaskSet，最后将TaskSet传递给TaskScheduler
	3.2接下来由TaskSchedule来处理，根据TsajSet中描述的线程的信息，将数据执行任务发送给executor来执行，尽可能保证每一个分区的Task运行在不同的executor上，确保资源最大化，整个资源申请都是由TaskSchedule完成的
```

```properties
一个spark应用程序，可以产生多个JOB任务（有多少个action算子），一个Job任务产生一个DAG执行流程图，一个DAG就会有多个stage阶段，一个stage阶段有多少线程
```

## Spark的并行度

Spark的并行度是决定Spark执行效率非常重要的因素，一般可以说并行度越高执行效率越高，前提是资源足够

```properties
在spark中并行度主要取决于以下两个因素：
	1-资源因素:由提交任务时候，所申请的executor的数量以及CPU核数和内存来决定
	2-数据因素:数据的大小，对应分区数量以及Task线程
描述：
	当申请的资源比较大的时候，如果数据量不大，这样虽然不会影响执行的效率，但是会导致资源的浪费
	当申请的资源比较小的时候，但是数据量比较大，会导致没有相应的资源来运行，本应该可以并行执行的操作，变成了串行执行，影响整个执行效率
	
如何调整并行度
	调整标准:在合适的资源上，运行合适的任务，产生合适的并行度
			除了可以给出一些经验值以外，更多还需要我们不断的调试
	建议值:一个CPU核数上运行2~3个线程，一个CPU对应内存大小为3~5GB
```



## 了解combineByKey

combineByKey是一个非常底层的算子，是aggregateByKey底层实现： 汇总处理

```properties
整体关系：
	combineByKey-->aggregateByKey-->flogByKey-->reduceByKey

使用格式：
	combineByKey(fn1,fn2,fn3)
	参数1:fn1 设置初始值
	参数2:fn2 对每个分区执行函数
	参数3:fn3 对每个分区执行完结果
```

代码演示：

```python
from pyspark import SparkContext, SparkConf
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("combinerByKey演示")

    # 1- 创建SparkContext对象
    conf = SparkConf().setMaster('local[*]').setAppName('sougou')
    sc = SparkContext(conf=conf)
    # 2- 初始化数据
    rdd_init = sc.parallelize(
        [('c01', '张三'), ('c02', '李四'), ('c01', '王五'), ('c01', '赵六'), ('c02', '田七'), ('c03', '周八'), ('c02', '李九')])
    # 需求:
    """
        要求将数据转换为以下格式: 
            [
                ('c01',['张三','王五','赵六'])
                ('c02',['李四','田七','李九'])
                ('c03',['周八'])
            ]
    """
     # 3- 处理数据
    def fn1(agg):
        return [agg]


    def fn2(agg, curr):
        agg.append(curr)
        return agg


    def fn3(agg, curr):
        print(agg)
        agg.extend(curr)
        return agg


    rdd_res = rdd_init.combineByKey(fn1, fn2, fn3)

    print(rdd_res.collect())
```













