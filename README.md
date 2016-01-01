### Run task on Spark Yarn cluster

Create tasks directory at Yarn master node
```
cd $SPARK_HOME
mkdir tasks
```

Include HBase jars on the Spark class path. Add following line to the conf/spark-defaults.conf
```
spark.executor.extraClassPath=/opt/cloudera/parcels/CDH-5.4.7-1.cdh5.4.7.p0.3/lib/hbase/lib/*
```

Build the project and copy jar to the server
```
cd $EVENTS_HOME
sbt clean assembly
scp target/scala-2.10/events-fetcher-assembly-0.0.1.jar root@192.168.56.221:/$SPARK_HOME/tasks
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


# GIT
### git diff
show differences between working tree and index, changes you haven't staged to commit
```
git diff [filename]
```

show differences between index and current commit, changes you're about to commit
```
git diff --cached [filename]
```

show differences between working tree and current commit
```
git diff HEAD [filename]
```

### git add
Don’t actually add the file(s), just show if they exist and/or will be ignored.
```
git add . --dry-run
```