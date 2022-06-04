# Spark SQL 分布式执行引擎

​	目前, 我们已经完成了spark集成hive的操作, 但是目前集成后, 如果需要连接hive, 此时需要启动一个spark的客户端(pyspark,spark-sql, 或者代码形式)才可以, 这个客户端底层, 相当于启动服务项, 用于连接hive服务, 进行处理操作,  一旦退出了这个客户端, 相当于这个服务也不存在了, 同样也就无法使用了

​	此操作非常类似于在hive部署的时候, 有一个本地模式部署(在启动hive客户端的时候, 内部自动启动了一个hive的hiveserver2服务项)

```properties
大白话: 
	目前后台没有一个长期挂载的spark的服务(spark hiveserver2 服务), 导致每次启动spark客户端,都先在内部构建一个服务项, 这种方式 ,仅仅适合于测试, 不适合后续开发
```

## 如何启动spark的分布式执行引擎呢?  这个引擎可以理解为 spark的hiveserver2服务

```properties
cd /export/server/spark

./sbin/start-thriftserver.sh \
--hiveconf hive.server2.thrift.port=10000 \
--hiveconf hive.server2.thrift.bind.host=node1 \
--hiveconf spark.sql.warehouse.dir=hdfs://node1:8020/user/hive/warehouse \
--master local[*]
```

![1654147202777](20、Spark SQL分布式执行引擎.assets/1654147202777.png)

启动后: 可以通过 beeline的方式, 连接这个服务, 直接编写SQL即可:

```properties
cd /export/server/spark/bin
./beeline

输入:
!connect jdbc:hive2://node1:10000
```

![1654147252721](20、Spark SQL分布式执行引擎.assets/1654147252721.png)

相当于模拟了一个HIVE的客户端, 但是底层运行是spark SQL 将其转换为RDD来运行的



## 方式二:  如何通过 datagrip 或者 pycharm 连接 spark进行操作:

![1654147374546](20、Spark SQL分布式执行引擎.assets/1654147374546.png)

![1654147385953](20、Spark SQL分布式执行引擎.assets/1654147385953.png)

![1654147398800](20、Spark SQL分布式执行引擎.assets/1654147398800.png)

![1654147411137](20、Spark SQL分布式执行引擎.assets/1654147411137.png)

![1654147421320](20、Spark SQL分布式执行引擎.assets/1654147421320.png)

![1654147430601](20、Spark SQL分布式执行引擎.assets/1654147430601.png)

![1654147441040](20、Spark SQL分布式执行引擎.assets/1654147441040.png)

![1654147450688](20、Spark SQL分布式执行引擎.assets/1654147450688.png)