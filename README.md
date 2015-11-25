

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
