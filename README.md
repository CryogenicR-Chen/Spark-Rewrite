# Spark-Rewrite
在sloth-commerce-test1.jd.163.org <br>
内存  = --executor-memory 4505m *  --num-executors 5 * --executor-cores 1 <br>
-c iceberg_catalog4 设置catalog   -s db4551 设置库 <br>
-m rewrite 这个参数没用，写上就行 <br>
-f 1 rewrite循环频率，单位是秒 <br>
-a 12张表rewrite <br>
如果没有-a的话 -t table_name 参数可以指定rewrite某一张表 <br>
-p 的取值为0-5，分别对应了以下sql，根据你需要的sql选择 <br>
String.format("CALL %s.system.rewrite_data_files('%s.%s')", CATALOG, DB, localTableName); <br>
String.format("CALL %s.system.rewrite_data_files(table => '%s.%s', options => map('min-input-files','2','rewrite-all','true'))", CATALOG, DB, localTableName); <br>
String.format("CALL %s.system.rewrite_data_files(table => '%s.%s', options => map('min-input-files','2','rewrite-all','false'))", CATALOG, DB, localTableName); <br>
String.format("CALL %s.system.rewrite_data_files(table => '%s.%s', options => map('delete-file-threshold','5'))", CATALOG, DB, localTableName); <br>
String.format("CALL %s.system.rewrite_data_files(table => '%s.%s', options => map('delete-file-threshold','5','rewrite-all','true'))", CATALOG, DB, localTableName); <br>
String.format("CALL %s.system.rewrite_data_files(table => '%s.%s', options => map('rewrite-all','true'))", CATALOG, DB, localTableName); <br>

```
unset SPARK_HOME
unset SPARK_CONF_DIR
unset HADOOP_CONF_DIR
export SPARK_HOME=/home/arctic/spark/spark-3.3.2-bin-hadoop2 
export SPARK_CONF_DIR=/home/arctic/spark/spark-3.3.2-bin-hadoop2/conf
export YARN_CONF_DIR=/home/arctic/spark/conf
export HADOOP_CONF_DIR=/home/arctic/spark/conf
cd /home/arctic/spark/spark-3.3.2-bin-hadoop2/examples/jars

/home/arctic/spark/spark-3.3.2-bin-hadoop2/bin/spark-submit  --master yarn --deploy-mode client --num-executors 5 --executor-memory 4505m --executor-cores 1 --class org.rewrite.SparkRewrite /home/arctic/spark/spark-3.3.2-bin-hadoop2/examples/jars/spark-rewrite-1.7-SNAPSHOT.jar -c iceberg_catalog4 -s db4551 -a -m rewrite -f 1 -p 5
```
