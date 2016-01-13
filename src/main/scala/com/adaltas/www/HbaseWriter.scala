package com.adaltas.www

import java.util

import org.apache.hadoop.hbase.client.{HTableInterface, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD

/**
  * Created by Bakalian on 25/11/15.
  */
class HbaseWriter {

  /**
    * insert single line into hbase
    * @param rowkey the line rowkey
    * @param qualifier column's family qualifer
    * @param message value of content
    * @param cf column's family name
    * @param table the target table
    */
  def insertToHbase(rowkey: String, qualifier:String, message: String, cf: String, table: HTableInterface ) : Unit = {


    //defines the key
    val put = new Put(Bytes.toBytes(rowkey))
    put.add(Bytes.toBytes(cf), Bytes.toBytes(qualifier), Bytes.toBytes(message))

    table.put(put)
    println("written single line to hbase "+ message)
  }

  /**
    *
    * bulk load into hbase from received RDD
    * @param rowkey the line rowkey
    * @param qualifier column's family qualifer
    * @param message value of content
    * @param cf column's family name
    * @param table the target table
    */
  def insertToHbase(rowkey: String, qualifier:String, message: RDD[(String, String)], cf: String, table: HTableInterface ) : Unit = {

    //defines the key
    val puts = new util.ArrayList[Put]()
    println(message)
    println("--")
    message.foreach(println)
    println("--")
    message.foreach(x => println(x))
    println("--")
    message.collect().foreach(x => {
      var counter = 0
      if((x._1 != null)){
        println(rowkey+"-"+x._1.toString + cf + qualifier + x._1.toString + "--|--" +x._2.toString)
        val put = new Put(Bytes.toBytes(rowkey+"-"+x._1.toString))
        put.add(Bytes.toBytes(cf), Bytes.toBytes(qualifier), Bytes.toBytes(x._1.toString + "--|--" +x._2.toString))
        puts.add(put)
      }else
      {
        println("nu,ber of message"  + counter)
        val put = new Put(Bytes.toBytes(rowkey+"kafka empty message"))
        put.add(Bytes.toBytes(cf), Bytes.toBytes(qualifier), Bytes.toBytes(counter.toString))
        puts.add(put)
        counter = counter + 1
      }


    })
    println(puts)
    table.put(puts)
    println("bulk load written")

  }

  def insertOneLineToHbase(rowkey: String, qualifier:String, message: String, cf: String, table: HTableInterface ) : Unit = {

        val put = new Put(Bytes.toBytes(rowkey))
        put.add(Bytes.toBytes(cf), Bytes.toBytes(qualifier), Bytes.toBytes(message))
        table.put(put)

  }

}
