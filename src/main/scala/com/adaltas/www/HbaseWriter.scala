package com.adaltas.www

import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes

/**
  * Created by Bakalian on 25/11/15.
  */
class HbaseWriter {

  def insertToHbase(rowkey: String, qualifier:String, message: String, cf: String, table: HTable ) : Unit = {


    //defines the key
    val put = new Put(Bytes.toBytes(rowkey))
    put.add(Bytes.toBytes(cf), Bytes.toBytes(qualifier), Bytes.toBytes(message))
    table.put(put)
  }
}
