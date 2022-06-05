source /etc/profile
echo '1/6,关闭hadoop..'
stop-all.sh
echo '2/6,关闭hiveserver2|metastore|HiveThriftServer2..'
echo `ps -ef | grep -E 'hiveserver2|metastore|HiveThriftServer2'`
y=`ps -ef | grep -E 'hiveserver2|metastore|HiveThriftServer2' | awk -F' ' '{print $2}'`
arr=(${y})
sum=1
for x in $y
do 
if [ $sum -lt ${#arr[*]} ]; then
    echo $sum,关闭进程号$x
    sum=$[ $sum + 1 ]
    kill -9 $x
fi
done
echo '3/6,启动hadoop..'
start-all.sh
echo '4/6,启动hive-metastore..'

nohup /export/server/hive/bin/hive --service metastore  2>&1 > /tmp/hive-metastore.log &
echo '5/6,启动hive-hiveserver2..'
nohup /export/server/hive/bin/hive --service hiveserver2 2>&1 > /tmp/hive-hiveserver2.log &
echo '6/6.启动spark-thriftserver..'
/export/server/spark/sbin/start-thriftserver.sh \
 --hiveconf hive.server2.thrift.port=10001 \
 --hiveconf hive.server2.thrift.bind.host=node1 \
 --master local[*]
