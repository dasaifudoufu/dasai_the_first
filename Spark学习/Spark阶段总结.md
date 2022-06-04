# Spark阶段总结

## 大纲概览

- Spark概述

- Spark base
- Spark core
- Spark SQL
- 综合案例

## Spark概述

### 产生背景

MR不能满足需求

### Spark的特点及优势

保留了MR的优点,更好的解决了迭代计算问题,提供了一定的流式计算能力,新增了图形计算和机器学习的模块

### Spark的发展历程

### Spark的应用

主要应用于离线数仓,正在逐步取代HIVE的主导地位.

## Spark base

### 概述

### 搭建

### 应用

### Spark on yarn环境配置

### Spark与PySpark交互

### spark-submit的相关参数描述

## Spark core

### 概述

### 核心:全新的数据集--RDD

### 执行原理

### RDD相关

#### RDD定义

#### RDD特点与优势

#### RDD原理

#### RDD分类

#### RDD算子

#### RDD的持久化

#### 共享变量:广播变量与累加器

### Spark的内核调度

#### RDD的依赖关系

#### shuffle

#### DAG和stage

#### Driver的调度机制

#### RDD的并行度设置

#### 了解 combinerByKey

## Spark SQL

### 概述

### 核心:dataFrame

### 执行原理

### 引入pandas

### 语法

## 综合案例





