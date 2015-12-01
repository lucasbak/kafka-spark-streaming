
# Build, Deploy

You can build, deploy the spark streamer by running the script or by executing the command manually. Replace kerberos principal value and/or broker list for both methods.

* With the bin
  ```bash
  bin/stream
  ```

* Manually

- Build the project
  It will build the project and create an uberized jar containing all the needed dependencies.
  ```bash
    mvn package
  ```
- Deploy the spark uberized jar
  ```bash
   scp ./target/kafka_spark_streaming-1.0-jar-with-dependencies.jar root@front1.ryba:/root/
  ```

# Run the JOB

The Job reads data from Kafka and write to Kafka and/or to HBase

## Writing to kafka

- Launch the spark streaming consumer/producer program with input topic 'input_topic' and output topic 'output_topic'

    ```bash
    kinit tester@REALM
    /usr/hdp/current/spark-client/bin/spark-submit \
    --master yarn-cluster \
    /root/kafka_spark_streaming-1.0-jar-with-dependencies.jar \
    -input_topic input_topic \
    -b "master1.ryba:9092,master2.ryba:9092,master3.ryba:9092"
    -output_topic my_output_topic
    ```
- Start the kafka consumer console (for reading from spark job output)
  ```
  /usr/hdp/current/kafka-broker/bin/kafka-console-consumer.sh \
  --topic "input_topic" \
  --zookeeper master1.ryba:2181,master2.ryba:2181,master2.ryba:2181 \
  --from-beginning
  ```

- Start the kafka-perf tester (for creating spark Dstream input )
  ```bash
  /usr/hdp/current/kafka-broker/bin/kafka-producer-perf-test.sh \
  --messages 100 \
  --topic input_topic \
  --request-num-acks 1
  ```

# Writing to HBase ( kerberos ) cluster

## How it works

- Description

  Since Spark 1.4.1, Spark Jobs can interact with hbase kerberized cluster by fetching and using needed token.
  For this it uses the runtime reflection scala mechanism ( introduced in 2.10 ) to load the Hbase TokenUtil Class and
  obtain Hbase delegation token ( same for Hive Metastore ).
  Nevertheless it has been impossible to use it for now due to HBase Class not found (org.apache.hadoop.hbase.HBaseConfiguration), which prevent
  spark driver and executor to open a connection with HBase Server.
  A similar [issue][hive-spark] has been opened for Hive and corrected for Spark 1.5.3 and 1.6.0. No issue opened for HBase now.
  As a work around, the spark job fetches delegation token it self for interacting with HBase.

  This posts assumes that you job user submitter is already created and his keytab deployed on every worker node and have hbase-site.xml  and core-site.xml files on every node of your cluster with read permission for everyone.
  spark is configured and works in yarn cluster mode

- Deploy Keytabs (if kerberized)

  Two modes are possible (for backward compatibility designed to run on spark from 1.3.1)
  * Deploy the keytab your self on every node managers, at the location that you will specifiy  with -keytab option
  * Put the keytab in hdfs. With this method the location should start by 'hdfs:'   (e.g. hdfs://hdfs_namenode:8020/user/tester/tester.keytab)


- Run the command
The Spark Job will write to hbase table_message, at 'cf1' column family
-bulk is optional. It writes the content of each kafka message into a separate table
(e.g. table_bulk)

  ```bash
  kinit tester@REALM.COM
  /usr/hdp/current/spark-client/bin/spark-submit \
  --master yarn-cluster \
  /root/kafka_spark_streaming-1.0-jar-with-dependencies.jar \
  -input_topic input_topic \
  -b "master1.ryba:9092,master2.ryba:9092,master3.ryba:9092" \
  -input_topic my_input_topic \
  -principal tester@REALM.COM \
  -keytab /path/to/keytab \
  -z host1,host2,host2 \
  -zp 2181
  -table table_message
  -bulk table_bulk
  ```

## Debugging

These instructions might not been complete according to how you set up your cluster. These are the main steps I needed to execute to make it work.

### Kerberos

  When creating a job submitter user be sure to follow this steps:

  * Create a kerberos principal

   ```bash
    add_principal tester@REALM.COM
   ```
  * generate a keytab for this principal
   ```bash
   ktadd -k /path/to/tester.keytab tester@REALM.COM
   ```
  * deploy the keytab on every worker node or in hdfs

### User  

  Create the user ( and allowing to submit to yarn)
    ```bash
    useradd tester -g hadoop
    ```
### HDFS

  Create hdfs layout for user.
    ```bash
    hdfs dfs -mkdir /user/tester
    hdfs dfs -chown -R tester:tester /user/tester
    ```

### HBase

  * ACL's - Create a table with enough permission for the user"
  ```bash
  kinit hbase@REALM.COM
  hbase shell >$ create "table_tester", { NAME => 'cf1' }
  hbase shell >$ grant 'tester', 'RWC', 'table_tester'
  ```

  * Hbase-site
  The Hbase site has to contain the master, and region server principal.
  it must contain the following properties
    - "hbase.rpc.controllerfactory.class"
    - "hbase.zookeeper.quorum"
    - "hbase.zookeeper.property.clientPort"
    - "hadoop.security.authentication"
    - "hbase.security.authentication"
  the hbase.zookeeper.quorum and hbase.zookeeper.property.clientPort
  can be passed to command line as -z, --zk_quorum  and -zp, --zk_port

### Hadoop conf

  Be sure to have following file deployed on every yarn node managers:
  - hbase-site.xml in /etc/hbase/conf/hbase-site.xml
  - core-site in /etc/hadoop/conf/

### Spark conf

spark-default.conf and spark-env.sh should exist and rightly configured in /etc/spark/conf/

[ryba-io]:(https://github.com/ryba-io/ryba)
[hive-spark]:(https://issues.apache.org/jira/browse/SPARK-11265)
