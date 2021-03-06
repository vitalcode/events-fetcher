Spark task that extracts, categorises and builds elastic index for events data crawled with events-crawler.


### Run task on Spark cluster (Yarn mode)

Create tasks directory at master node
```
cd $SPARK_HOME
mkdir tasks
```

Include HBase jars on the Spark class path. Add following line to the conf/spark-defaults.conf
```
spark.executor.extraClassPath=/opt/cloudera/parcels/CDH-5.4.7-1.cdh5.4.7.p0.3/lib/hbase/lib/*
```

Set $ZOOKEEPER_QUORUM and $ES_NODES for Spark executor worker nodes
```
export ES_NODES=vps1234.net:9200
export ZOOKEEPER_QUORUM=vps1234.net,vps5678.net.net
```

Build the project and copy jar to the server
```
cd $EVENTS_HOME
sbt clean assembly
scp target/scala-2.10/events-fetcher-assembly-0.0.1.jar user@host:/$SPARK_HOME/tasks
```

Submit spark task. 
Need to supply path to the hbase lib directory to put hbase jars on driver path
```
./bin/spark-submit --class uk.vitalcode.events.fetcher.Client \
    --master yarn-client \
    --num-executors 1 \
    --driver-memory 300m \
    --executor-memory 300m \
    --executor-cores 1 \
    --queue thequeue \
    --driver-class-path /opt/cloudera/parcels/CDH-5.4.7-1.cdh5.4.7.p0.3/lib/hbase/lib/*: \
    tasks/events-fetcher-assembly-0.0.1.jar
```
    
    
### Run tests
sbt test
