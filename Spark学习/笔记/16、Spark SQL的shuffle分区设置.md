# Spark SQL的shuffle分区设置

## 需求

```properties
	spark SQL在执行的过程中, 会将SQL翻译为Spark的RDD程序来运行, 对于Spark SQL来说, 执行的时候, 同样也会触发shuffle操作, 默认情况下, Spark SQL的shuffle的分区数量默认为 200个

	在实际生产中使用的时候, 默认为200个 有时候并不合适, 当任务比较大的时候, 一般可能需要调整分区数量更多, 当任务较小的时候, 可能需要调小分区数量

注意:区分同一个stage内的分区设置,与两个stage(shuffle)情况下分区设置
```

```properties
如何来调整spark SQL的分区数量呢?  参数: spark.sql.shuffle.partitions
方案一: 在配置文件中调整:  spark-defaults.conf   (全局的方式)
	添加以下配置: 
		 spark.sql.shuffle.partitions   100

方案二:  在通过spark-submit 提交任务的时候, 通过 --conf "spark.sql.shuffle.partitions=100"  主要在生产中

方案三:  在代码中, 当构建SparkSession对象的时候, 来设置shuffle的分区数量:    主要是在测试中
		sparkSession.builder.config("spark.sql.shuffle.partitions",'100')
在测试环境中, 一般都是右键运行, 此时需要设置分区数量, 可以通过方案三来处理, 但是在后续上线部署的时候, 需要通过spark-submit提供, 为了能够让参数动态传递, 会将代码中参数迁移到 spark-submit命令上设置
```



