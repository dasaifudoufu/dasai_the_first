# Spark 程序与PySpark交互流程

## 两种方式

### Client客户端模式

### Cluster集群模式

## 交互流程理解

### 提交到Spark集群

#### Client模式

流程：

```properties
1-启动Driver程序
2-向Master申请资源
3-Master根据要申请的资源，返回对应资源列表
	executor1: node1 1核 1GB    
	executor2: node3 1核 1GB
4-连接对应的worker节点，通知他们启动Executor，当我worker启动完成后，需要反向注册回Driver(通知)
5-Driver开始执行Main函数：
	5.1初始化sc对象:构建SparkContext, 基于py4j. 将python中定义的如何初始化sc对象的代码转换为java代码进行执行处理
	5.2 当Driver程序构建好SC对象后, 开始将后续的所有的RDD相关的算子全部合并在一起, 根据rdd之间的依赖的关系, 形成一个DAG执行流程图 , 划分出一共有多少个阶段 以及每个阶段需要运行多少个线程, 最后每个线程需要运行在那个executor上 (任务分配)
	5.3 当确定每个节点中有多少个线程 以及每个线程应该运行在那个executor后, 接下来将这些任务信息发送给对应executor, 让其进行执行处理
	5.4 当executor接收到任务后, 根据任务信息, 开始运行处理, 当Task运行完成后, 如果需要将结果返回给Driver(比如: 执行collect()),直接通过网络返回执行, 如果不需要返回(执行:saveasTextFile()),线程直接处理, 当内部Task执行完成后, executor会通知Driver已经执行完成了
	5.5 当Driver程序接收到所有的节点都运行完成后, 执行 后续的非RDD的代码, 并最终将sc对象关闭, 通知Master执行完成了, Master回收资源
	
```

#### Cluster模式

```properties
1- 首先会先将任务信息提交到Master主节点上
2- 当Master收到任务信息后, 首先会根据Driver的资源信息, 随机找一台worker节点用于启动Driver程序,并将任务信息交给Driver
3- 当对应worker节点收到请求后, 开启启动Driver, 启动后会和Master保持心跳机制,告知Master启动成功了, 启动后立即开始申请资源(executor)
4- Master根据要申请的资源, 返回对应资源列表    
	executor1 : node1 1核 1GB    
	executor2:  node3 1核 1GB
5- 连接对应worker节点, 通知他们启动Executor,当我worker启动完成后, 需要反向注册回Driver(通知)
6- Driver开始执行Main函数:
	6.1 初始化sc对象: 构建SparkContext, 基于py4j. 将python中定义的如何初始化sc对象的代码转换为java代码进行执行处理
	6.2 当Driver程序构建好SC对象后, 开始将后续的所有的RDD相关的算子全部合并在一起, 根据rdd之间的依赖的关系, 形成一个DAG执行流程图 , 划分出一共有多少个阶段 以及每个阶段需要运行多少个线程, 最后每个线程需要运行在那个executor上 (任务分配)
	6.3 当确定每个节点中有多少个线程 以及每个线程应该运行在那个executor后, 接下来将这些任务信息发送给对应executor, 让其进行执行处理
	6.4 当executor接收到任务后, 根据任务信息, 开始运行处理, 当Task运行完成后, 如果需要将结果返回给Driver(比如: 执行collect()),直接通过网络返回执行, 如果不需要返回(执行:saveasTextFile()),线程直接处理, 当内部Task执行完成后, executor会通知Driver已经执行完成了
	6.5 当Driver程序接收到所有的节点都运行完成后, 执行 后续的非RDD的代码, 并最终将sc对象关闭, 通知Master执行完成了, Master回收资源
```

### 提交到yarn集群

#### Client模式

```properties
1- 启动Driver程序
2- 连接Yarn的主节点(resourceManager),向主节点提交一个资源的任务(目标: 启动executor)
3- Yarn的resourceManager接收到任务后, 开始随机在某一个nodemanager节点上启动appMaster, AappMaster启动后会和resourceManager建立心跳机制, 报告已经启动完成了
4- AppMaster根据资源信息要求,向resourceManager申请资源, 通过心跳的方式将申请资源信息发送到RM,然后不断的询问RM是否已经准备好了
5- 当AppMaster一旦检测到RM中已经将资源准备好了,那么就会立即将资源分配结果信息获取到, 根据分配资源信息在对应的nodemanager节点上启动进程(executor)
6- 当executor启动完成后, 通知appMaster 同时反向注册到Driver端(通知)
7- Driver收到各个executor的注册信息后, 开始执行Main函数:
	7.1 初始化sc对象: 构建SparkContext, 基于py4j. 将python中定义的如何初始化sc对象的代码转换为java代码进行执行处理
	7.2 当Driver程序构建好SC对象后, 开始将后续的所有的RDD相关的算子全部合并在一起, 根据rdd之间的依赖的关系, 形成一个DAG执行流程图 , 划分出一共有多少个阶段 以及每个阶段需要运行多少个线程, 最后每个线程需要运行在那个executor上 (任务分配)
	7.3 当确定每个节点中有多少个线程 以及每个线程应该运行在那个executor后, 接下来将这些任务信息发送给对应executor, 让其进行执行处理
	7.4 当executor接收到任务后, 根据任务信息, 开始运行处理, 当Task运行完成后, 如果需要将结果返回给Driver(比如: 执行collect()),直接通过网络返回执行, 如果不需要返回(执行:saveasTextFile()),线程直接处理, 当内部Task执行完成后, executor会通知Driver已经执行完成了, 同时会执行完成的状态通知appMaster, AppMaster收到全部的节点都结果完成后, 通知RM 任务已经执行完成, RM通知appMaster可以退出(自杀过程),并且回收yarn的资源
	7.5 当Driver程序接收到所有的节点都运行完成后, 执行 后续的非RDD的代码, 并最终将sc对象关闭
```

#### Cluster

```properties
1- 首先会先将任务信息提交到resourceManager主节点上
2- 当resourceManager收到任务信息后, 首先会根据Driver的资源信息, 随机找一台worker节点用于启动appMaster(Driver)程序,并将任务信息交给Driver
3- 当对应worker节点收到请求后, 开启启动appMaster(Driver), 启动后会和resourceManager保持心跳机制,告知resourceManager启动成功了, 启动后立即开始申请资源(executor)
4- resourceManager根据要申请的资源, 返回对应资源列表    
	executor1 : node1 1核 1GB    
	executor2:  node3 1核 1GB
5- 连接对应worker节点, 通知他们启动Executor,当我worker启动完成后, 需要反向注册回resourceManager(通知)
6- appMaster(Driver)开始执行Main函数:
	6.1 初始化sc对象: 构建SparkContext, 基于py4j. 将python中定义的如何初始化sc对象的代码转换为java代码进行执行处理
	6.2 当appMaster(Driver)程序构建好SC对象后, 开始将后续的所有的RDD相关的算子全部合并在一起, 根据rdd之间的依赖的关系, 形成一个DAG执行流程图 , 划分出一共有多少个阶段 以及每个阶段需要运行多少个线程, 最后每个线程需要运行在那个executor上 (任务分配)
	6.3 当确定每个节点中有多少个线程 以及每个线程应该运行在那个executor后, 接下来将这些任务信息发送给对应executor, 让其进行执行处理
	6.4 当executor接收到任务后, 根据任务信息, 开始运行处理, 当Task运行完成后, 如果需要将结果返回给Driver(比如: 执行collect()),直接通过网络返回执行, 如果不需要返回(执行:saveasTextFile()),线程直接处理, 当内部Task执行完成后, executor会通知Driver已经执行完成了
	6.5 当appMaster(Driver)程序接收到所有的节点都运行完成后, 执行 后续的非RDD的代码, 并最终将sc对象关闭, 通知Master执行完成了, Master回收资源
```



