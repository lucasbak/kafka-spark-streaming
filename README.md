

# Build, Deploy and run

You can build, deploy and start the spark streamer by running the script or by executing the command manually. Replace kerberos principal value and/or broker list for both methods.

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
   scp ./target/ kafka_spark_streaming-1.0-jar-with-dependencies.jar root@front1.ryba:/root/

  ```
- Launch the spark streaming consumer/producer program with input topic 'input_topic' and output topic 'output_topic'
    ```bash
    echo password | kinit principal@REALM {
    /usr/hdp/current/spark-client/bin/spark-submit \
    --master yarn-cluster \
    /root/kafka_spark_streaming-1.0-jar-with-dependencies.jar \
    -input_topic input_topic \
    -b "master1.ryba:9092,master2.ryba:9092,master3.ryba:9092"
  }
    ```
- Start the kafka consumer console
  ```
  /usr/hdp/current/kafka-broker/bin/kafka-console-consumer.sh \
  --topic "input_topic" \
  --zookeeper master1.ryba:2181,master2.ryba:2181,master2.ryba:2181 \
  --from-beginning
  ```

- Start the kafka-perf tester
  ```bash
  /usr/hdp/current/kafka-broker/bin/kafka-producer-perf-test.sh \
  --messages 100 \
  --topic input_topic \
  --request-num-acks 1
  ```

  # Using HBase secured mode

  ## Description

  Since Spark 1.4.1, Spark Jobs can interact with hbase kerberized cluster by fetching and using needed token.
  For this it uses the runtime reflection scala mechanism ( introduced in 2.10 ) to load the Hbase TokenUtil Class and
  obtain Hbase delegation token ( same for Hive Metastore ).
  Nevertheless it has been impossible to use it for now due to HBase Class not found (org.apache.hadoop.hbase.HBaseConfiguration), which prevent
  spark driver and executor to open a connection with HBase Server.
  A similar [issue][hive-spark] has been opened for Hive and corrected for Spark 1.5.3 and 1.6.0. No issue opened for HBase now.
  As a work around, the spark job fetches delegation token it self for interacting with HBase.

  This posts assumes that you job user submitter is already created and his keytab deployed on every worker node. Or you can follow following steps:

  ## Help

  * Create a kerberos principal

   ```bash
    add_principal tester@REALM.COM
   ```
  * generate a keytab for this principal
   ```bash
   ktadd -k /path/to/tester.keytab tester@REALM.COM
   ```
  * deploy the keytab on every worker node
    ```bash
    scp  /path/to/tester.keytab login@worker_host:/path/to/tester.keytab
    chmod 0700 /path/to/tester.keytab
    ```
  * create the user ( and allowing to submit to yarn)
    ```bash
    useradd tester -g hadoop
    ```

  * don't forget to create a table with enough permission for the user which will write to HBase.
    ```bash
    # HBase shell
    kinit hbase@REALM.COM
    create 'table_tester', { NAME => 'cf1'}
    grant 'tester', 'RWC', 'table_tester'
    ```

[hive-spark]:(https://issues.apache.org/jira/browse/SPARK-11265)
