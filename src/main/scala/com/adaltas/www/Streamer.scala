package com.adaltas.www

import java.io.File
import java.net.URLClassLoader
import java.nio.charset.StandardCharsets._
import java.security.PrivilegedAction
import java.text.{SimpleDateFormat, DateFormat}
import java.util.{Date, Properties}

import org.apache.hadoop.hbase.security.UserProvider
import org.apache.hadoop.hbase.zookeeper.ZKUtil
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.spark.deploy.SparkHadoopUtil
import scala.reflect.runtime.universe
import org.apache.hadoop.io.Text
import _root_.kafka.producer.Producer
import _root_.kafka.producer.ProducerConfig
import _root_.kafka.serializer.StringDecoder
import org.apache.commons.cli.BasicParser
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.CommandLineParser
import org.apache.commons.cli.HelpFormatter
import org.apache.commons.cli.Options

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.security.token.TokenUtil
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.token.{TokenIdentifier, Token}


import org.apache.spark.{Logging, SparkConf}

import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import scala.reflect.runtime.{universe => ru}

/**
 * @author Lucas bkian
 */
object Streamer extends Logging{


  def main(args : Array[String]) {


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
    main_options.addOption("master_p","master_principal", true, " hbase.master.kerberos.principal  ")
    main_options.addOption("region_p","region_principal", true, " hbase.regionserver.kerberos.principal")
    main_options.addOption("r","realm", true, " e.g. HADOOP.RYBA")
    main_options.addOption("table","hbase_table", true, " the hbase table to write to, no output written if not specified")

    val parser: CommandLineParser = new BasicParser
    val cmd: CommandLine = parser.parse(main_options, args)

    if (cmd.hasOption("help_kafka")) {

      val f: HelpFormatter = new HelpFormatter()
      f.printHelp("Usage", main_options)
      System.exit(-1)
    }

    else {

      /**
        * Context Configuration & Creation
        */
//      val conf = new SparkConf().setAppName("Spark Streamer").setExecutorEnv("KRB5CCNAME","FILE:/tmp/krb5cc_2415").setExecutorEnv("java.security.krb5.conf","/etc/krb5.conf").setExecutorEnv("sun.security.krb5.debug","true")
      val conf = new SparkConf().setAppName("Spark Streamer")
      val ssc = new StreamingContext(conf, Seconds(2))

      /**
        * HBase & kerberos Configuration
        */
      System.setProperty("java.security.krb5.conf", "/etc/krb5.conf")
      System.setProperty("sun.security.krb5.debug", "true")
//      System.setProperty("KRB5CCNAME","FILE:/tmp/krb5cc_2415")
      val hbase_conf: Configuration = HBaseConfiguration.create()


      hbase_conf.addResource(new Path("/etc/hbase/conf/core-site.xml"))
      hbase_conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"))

      hbase_conf.set("hbase.rpc.controllerfactory.class",  "org.apache.hadoop.hbase.ipc.RpcControllerFactory")
      hbase_conf.set("hbase.zookeeper.quorum", cmd.getOptionValue("z","master1.ryba,master2.ryba,master3.ryba"))
      hbase_conf.set("hbase.zookeeper.property.clientPort", cmd.getOptionValue("zp","2181"))
      hbase_conf.set("hadoop.security.authentication", "kerberos")
      hbase_conf.set("hbase.security.authentication", "kerberos")

      val master_princ = cmd.getOptionValue("master_p","hbase/_HOST") + "@" + cmd.getOptionValue("r","HADOOP.RYBA")
      val region_princ = cmd.getOptionValue("master_p","hbase/_HOST") + "@" + cmd.getOptionValue("r","HADOOP.RYBA")
      hbase_conf.set("hbase.master.kerberos.principal", master_princ)
      hbase_conf.set("hbase.regionserver.kerberos.principal",  region_princ)

      /**
        * commandline option parsing
        */

      val input_topics = cmd.getOptionValue("input_topic", "page_visits")
      /**
        * Kafka Configuration
        */
      val kafkaParams: Map[String, String] = Map[String, String]("metadata.broker.list" -> cmd.getOptionValue("b"))
      val topicsSet = input_topics.split(",").toSet
      /**
        * Kafka Producer Configuration
        */
      val props: Properties = new Properties
      props.put("metadata.broker.list", cmd.getOptionValue("b"))
      props.put("serializer.class", "kafka.serializer.StringEncoder")
      props.put("request.required.acks", "1")
      val config: ProducerConfig = new ProducerConfig(props)


      /**
        *  DStream Configuration and creation
        */
      val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
      // http://spark.apache.org/docs/1.4.1/streaming-programming-guide.html#output-operations
      // messages.foreachRDD( x => println(x))
      var counter = 0


      messages.foreachRDD { x =>
        val hbaseOutputWriter: HbaseWriter = new HbaseWriter()
        UserGroupInformation.setConfiguration(hbase_conf)
        val formatter: DateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm")
        counter += 1
        val current_date = formatter.format(new Date())
        /**
          * Writing producer message
          */
        // creating message
        val message = "Spark - date:" + current_date + " from topic: " + input_topics + " number of RDD (batches): "+  counter
        /**
          * writing to kafka
          */
        // creating producer
        val producer: Producer[String, String] = new Producer[String, String](config)
        //  creating kafka producer
        val KafkaOutputWriter: KafkaProducer = new KafkaProducer()
        KafkaOutputWriter.writeToKafka(cmd.getOptionValue("output_topic", "output_topic"), message, producer)

        /**
          * Writing to HBAse
          */
        if (cmd.hasOption("table")) {
          if (UserGroupInformation.isSecurityEnabled) {
            val loggedUGI: UserGroupInformation = UserGroupInformation.loginUserFromKeytabAndReturnUGI(cmd.getOptionValue("principal", "tester@HADOOP.RYBA"), cmd.getOptionValue("keytab", "/etc/security/keytabs/tester.keytab"))
            val c: Configuration = hbase_conf
            loggedUGI.doAs(new PrivilegedAction[Void] {
              override def run() = {
                try {
                  val admin: HBaseAdmin = new HBaseAdmin(c)
                  if (!admin.isTableAvailable(cmd.getOptionValue("table"))) {
                    val tableDesc: HTableDescriptor = new HTableDescriptor(TableName.valueOf(cmd.getOptionValue("table")))
                    admin.createTable(tableDesc)
                  }
                  // old HBase API usage
                  val hConnection: HConnection = HConnectionManager.createConnection(c)
                  val table: HTableInterface = hConnection.getTable(cmd.getOptionValue("table"))
                  val rowkey: String = String.valueOf(System.currentTimeMillis() / 1000)
                  hbaseOutputWriter.insertToHbase(rowkey, "message", message, "cf1", table)
//                  val connection: Connection = ConnectionFactory.createConnection(c)




                }
                null
              }

            })
          }
          else {
            // old HBase API usage
            val admin: HBaseAdmin = new HBaseAdmin(hbase_conf)
            if (!admin.isTableAvailable(cmd.getOptionValue("table"))) {
              val tableDesc: HTableDescriptor = new HTableDescriptor(TableName.valueOf(cmd.getOptionValue("table")))
              admin.createTable(tableDesc)
            }
            val hConnection: HConnection = HConnectionManager.createConnection(hbase_conf)
            val table: HTableInterface = hConnection.getTable(cmd.getOptionValue("table"))
            val rowkey: String = String.valueOf(System.currentTimeMillis() / 1000)
            hbaseOutputWriter.insertToHbase(rowkey, "message", message, "cf1", table)


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

