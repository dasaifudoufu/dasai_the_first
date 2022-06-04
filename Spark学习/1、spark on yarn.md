# Spark on yarn 环境构建

## Spark on yarn本质

​	将Spark任务的pyspark文件，经过Py4J转换，提交到Yarn的JVM中去运行

## 配置过程

参考配置文档《spark部署文档.doc》

### 修改spark-env.sh

```she
cd /export/server/spark/conf
cp spark-env.sh.template spark-env.sh
vim /export/server/spark/conf/spark-env.sh

#添加以下内容
HADOOP_CONF_DIR=/export/server/hadoop/etc/hadoop
YARN_CONF_DIR=/export/server/hadoop/etc/hadoop
SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=hdfs://node1:8020/sparklog/
Dspark.history.fs.cleaner.enabled=true"

同步到其他两台(单节点时候, 不需要处理)
cd /export/server/spark/conf
scp -r spark-env.sh node2:$PWD
scp -r spark-env.sh node3:$PWD

```

### 修改hadoop的yarn-site.xml

```shell
cd /export/server/hadoop-3.3.0/etc/hadoop/
vim /export/server/hadoop-3.3.0/etc/hadoop/yarn-site.xml

#添加以下内容:
<configuration>
    <!-- 配置yarn主节点的位置 -->
    <property>
        <name>yarn.resourcemanager.hostname</name>
        <value>node1</value>
    </property>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
    <!-- 设置yarn集群的内存分配方案(主要检查这一段有没有) -->
    
    <property>
        <name>yarn.nodemanager.resource.memory-mb</name>
        <value>20480</value>
    </property>
    <property>
        <name>yarn.scheduler.minimum-allocation-mb</name>
        <value>2048</value>
    </property>
    <property>
        <name>yarn.nodemanager.vmem-pmem-ratio</name>
        <value>2.1</value>
    </property>
    
    <!-- 开启日志聚合功能 -->
    <property>
        <name>yarn.log-aggregation-enable</name>
        <value>true</value>
    </property>
    <!-- 设置聚合日志在hdfs上的保存时间 -->
    <property>
        <name>yarn.log-aggregation.retain-seconds</name>
        <value>604800</value>
    </property>
    <!-- 设置yarn历史服务器地址 -->
    <property>
        <name>yarn.log.server.url</name>
        <value>http://node1:19888/jobhistory/logs</value>
    </property>
    <!-- 关闭yarn内存检查 -->
    <property>
        <name>yarn.nodemanager.pmem-check-enabled</name>
        <value>false</value>
    </property>
    <property>
        <name>yarn.nodemanager.vmem-check-enabled</name>
        <value>false</value>
    </property>
</configuration>

```

### 将其同步到其他两台

```shell
cd /export/server/hadoop/etc/hadoop
scp -r yarn-site.xml node2:$PWD
scp -r yarn-site.xml node3:$PWD

```

### Spark设置历史服务器地址

```shell
cd /export/server/spark/conf
cp spark-defaults.conf.template spark-defaults.conf
vim spark-defaults.conf
添加以下内容：
spark.eventLog.enabled                  true
spark.eventLog.dir                      hdfs://node1:8020/sparklog/
spark.eventLog.compress                 true
spark.yarn.historyServer.address        node1:18080
配置后, 需要在HDFS上创建 sparklog目录
hdfs dfs -mkdir -p /sparklog
```

### 设置日志级别

```shell
#同步到其他节点（不需要同步，spark只有单节点）
cd /export/server/spark/conf
scp -r spark-defaults.conf log4j.properties node2:$PWD
scp -r spark-defaults.conf log4j.properties node3:$PWD

```

### 配置依赖spark jar包

​	当Spark Application应用提交运行在YARN上时，默认情况下，每次提交应用都需要将依赖Spark相关jar包上传到YARN 集群中，为了节省提交时间和存储空间，将Spark相关jar包上传到HDFS目录中，设置属性告知Spark Application应用。

```shell
hadoop fs -mkdir -p /spark/jars/
hadoop fs -put /export/server/spark/jars/* /spark/jars/
```

修改spark-defaults.conf

```shell
cd /export/server/spark/conf
vim spark-defaults.conf
#添加以下内容:
spark.yarn.jars  hdfs://node1:8020/spark/jars/*
```

同步到其他节点（无需分发，spark只有一个单节点）

```shell
cd /export/server/spark/conf
scp -r spark-defaults.conf root@node2:$PWD
scp -r spark-defaults.conf root@node3:$PWD
```

### 启动服务

Spark Application运行在YARN上时，上述配置完成

启动服务：HDFS、YARN、MRHistoryServer和Spark HistoryServer，命令如下：

```shell
## 启动HDFS和YARN服务，在node1执行命令
start-dfs.sh
start-yarn.sh
或
start-all.sh
注意：在onyarn模式下不需要启动start-all.sh（jps查看一下看到worker和master）
## 启动MRHistoryServer服务，在node1执行命令
mr-jobhistory-daemon.sh start historyserver
## 启动Spark HistoryServer服务，，在node1执行命令
/export/server/spark/sbin/start-history-server.sh
```

### Spark HistoryServer服务WEB UI页面地址

```shell
http://node1:18080/
```

## 提交测试

​	先将圆周率PI程序提交运行在YARN上，命令如下：

```shell
SPARK_HOME=/export/server/spark
${SPARK_HOME}/bin/spark-submit \
--master yarn \
--conf "spark.pyspark.driver.python=/root/anaconda3/bin/python3" \
--conf "spark.pyspark.python=/root/anaconda3/bin/python3" \
${SPARK_HOME}/examples/src/main/python/pi.py \
10
```



## 启动Pyspark

/export/server/spark/bin/pyspark --master local[*]





