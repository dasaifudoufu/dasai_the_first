echo '1,关闭hadoop..'
stop-all.sh
echo '2,关闭hiveserver2|metastore|HiveThriftServer2..'
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

