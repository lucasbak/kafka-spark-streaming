package com.adaltas.www

import java.security.PrivilegedAction
import java.text.{SimpleDateFormat, DateFormat}
import java.util.{Date, Properties}

import kafka.producer.{Producer, ProducerConfig}
import kafka.serializer.StringDecoder
import org.apache.commons.cli._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.{TableName, HTableDescriptor, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{HTableInterface, HConnectionManager, HConnection, HBaseAdmin}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Bakalian on 12/01/16.
  */
object KafkaStreamerToHbase extends Logging {


  def main(args: Array[String]) {
    /**
      * command line parsing amd options
      */
    val main_options: Options = new Options
    main_options.addOption("help_kafka", false, "show this help")
    main_options.addOption("b", "brokers", true, "Broker nodes - Ex : server1:9092,server2:9092")
    main_options.addOption("z", "zk_quorum", true, "Zookeeper nodes - Ex : server1,server2")
    main_options.addOption("zp", "zk_quorum_port", true, "e.g. 2181, default to 2181")
    main_options.addOption("output_topic", true, "producer topic to listen (String)")
    main_options.addOption("input_topic", true, " list of topic to listen (Array|String)")
    main_options.addOption("master_p", "master_principal", true, " hbase.master.kerberos.principal  ")
    main_options.addOption("region_p", "region_principal", true, " hbase.regionserver.kerberos.principal")
    main_options.addOption("r", "realm", true, " e.g. HADOOP.RYBA")
    main_options.addOption("table", "hbase_table", true, " the hbase table to write to, no output written if not specified")
    main_options.addOption("principal", "user_principal", true, " the user principal")
    main_options.addOption("keytab", "user_keytab", true, " the user's pricipal's keytab")
    main_options.addOption("bulk", "bulk_table", true, " if specified write all the read kafka messages to the table ")

    val parser: CommandLineParser = new BasicParser
    val cmd: CommandLine = parser.parse(main_options, args)

    if (cmd.hasOption("help_kafka")) {
      val f: HelpFormatter = new HelpFormatter()
      f.printHelp("Usage", main_options)
      System.exit(-1)
    }

    else {
      //      val conf = new SparkConf().setAppName("Spark Streamer").setExecutorEnv("KRB5CCNAME","FILE:/tmp/krb5cc_2415").setExecutorEnv("java.security.krb5.conf","/etc/krb5.conf").setExecutorEnv("sun.security.krb5.debug","true")
      val conf = new SparkConf().setAppName("Spark Messager").set("spark.kryo.classesToRegister", "org.apache.hadoop.io.LongWritable,org.apache.hadoop.io.Text,org.apache.hadoop.conf.Configuration")
      val ssc = new StreamingContext(conf, Seconds(1))

      /**
        * HBase & kerberos Configuration
        */
      System.setProperty("java.security.krb5.conf", "/etc/krb5.conf")
      System.setProperty("sun.security.krb5.debug", "true")
      /**
        * commandline option parsing
        */
      val input_topics = cmd.getOptionValue("input_topic", "page_visits")
      /**
        * Kafka Configuration for creating input DStream
        */
      val broker_list = cmd.getOptionValue("b")
      val kafkaParams: Map[String, String] = Map[String, String]("metadata.broker.list" -> broker_list)
      val topicsSet = input_topics.split(",").toSet
      /**
        * Kafka Producer Configuration for writing output as producers
        */
      val props: Properties = new Properties
      props.put("metadata.broker.list", broker_list)
      props.put("serializer.class", "kafka.serializer.StringEncoder")
      props.put("request.required.acks", "1")
      /**
        * DStream Configuration and creation
        */
      val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
      // http://spark.apache.org/docs/1.4.1/streaming-programming-guide.html#output-operations

      System.out.println("coucou - 1")
      messages.foreachRDD { x =>
        x.foreachPartition{ y =>
          if (cmd.hasOption("output_topic")) {
            // creating producer
            val config_kafka_producers: ProducerConfig = new ProducerConfig(props)
            val producer: Producer[String, String] = new Producer[String, String](config_kafka_producers)
            //  creating kafka producer
            val KafkaOutputWriter: KafkaProducer = new KafkaProducer()
            KafkaOutputWriter.writeToKafka(cmd.getOptionValue("output_topic", "output_topic"), "has received "  + " records !!", producer)
            /**
              * HDFS KEYTAB DISTRIBUTION
              */
            var hdfs_path: Path = null
            var principal = ""
            var local_path: Path = null
            if (UserGroupInformation.isSecurityEnabled) {
              if (!cmd.hasOption("keytab") || !cmd.hasOption("principal")) {
                throw new Error(" You have to specify keytab and principal when security is enabled ")
              } else {
                hdfs_path = new Path(cmd.getOptionValue("keytab", ""))
                principal = cmd.getOptionValue("principal", "")
                local_path = new Path(System.getenv("PWD") + "/" + hdfs_path.getName.split("/").last)
                val hdfs_conf = new Configuration()
                val fileSystem = FileSystem.get(hdfs_conf)
                UserGroupInformation.getCurrentUser.doAs(new PrivilegedAction[Void] {
                  override def run() = {
                    try {
                      if (cmd.hasOption("keytab")) {
                        if (hdfs_path.toString.contains("hdfs:")) {
                          fileSystem.copyToLocalFile(hdfs_path, local_path)
                        }
                      }
                      null
                    }
                  }
                })
              }
            }
            /** * HBASE CONFIGURATION*/
              val table_name = cmd.getOptionValue("table","table_tester")
              val hbase_conf: Configuration = HBaseConfiguration.create()
              hbase_conf.set("hbase.rpc.controllerfactory.class", "org.apache.hadoop.hbase.ipc.RpcControllerFactory")
              hbase_conf.set("hbase.zookeeper.quorum", cmd.getOptionValue("z", "master1.ryba,master2.ryba,master3.ryba"))
              hbase_conf.set("hbase.zookeeper.property.clientPort", cmd.getOptionValue("zp", "2181"))
              hbase_conf.set("hadoop.security.authentication", "kerberos")
              UserGroupInformation.setConfiguration(hbase_conf)
              val hbaseOutputWriter: HbaseWriter = new HbaseWriter()
              if (UserGroupInformation.isSecurityEnabled) {
                // Authenticate as the USER and return the USER with VALID KERBEROS CREDENTIALS
                // val loggedUGI: UserGroupInformation = UserGroupInformation.loginUserFromKeytabAndReturnUGI(cmd.getOptionValue("principal", "tester@HADOOP.RYBA"), cmd.getOptionValue("keytab", "/etc/security/keytabs/tester.keytab"))
                val loggedUGI: UserGroupInformation = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, local_path.toString)
                val c: Configuration = hbase_conf
                // OPEN HBase connection with Previous USER
                loggedUGI.doAs(new PrivilegedAction[Void] {
                  override def run() = {
                    try {
                      // Check if the table exist create otherwise
                      val admin: HBaseAdmin = new HBaseAdmin(c)
                      if (!admin.isTableAvailable(table_name)) {
                        val tableDesc: HTableDescriptor = new HTableDescriptor(TableName.valueOf(table_name))
                        admin.createTable(tableDesc)
                      }
                      // old HBase API usage
                      val hConnection: HConnection = HConnectionManager.createConnection(c)
                      val table: HTableInterface = hConnection.getTable(table_name)
                      val rowkey: String = String.valueOf(System.currentTimeMillis() / 1000)
                      // line put
                      y.foreach(record => {
                        System.out.println("has received -> " + record)
                        hbaseOutputWriter.insertOneLineToHbase(rowkey, "messages", record.toString() , "cf1", table)

                      })
                    }
                    null
                  }
                })
              }
          }
        }
      }
      /**
        * spark streaming lifecycle
        */
      ssc.start()
      ssc.awaitTermination()
    }
  }
}