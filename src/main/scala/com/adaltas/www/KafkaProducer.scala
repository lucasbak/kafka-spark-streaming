package com.adaltas.www
import _root_.kafka.producer.KeyedMessage
import _root_.kafka.producer.Producer

class KafkaProducer {


  def writeToKafka(topic: String, message: String, producer: Producer[String, String]): Unit = {
    val data: KeyedMessage[String, String] = new KeyedMessage[String, String](topic, message)
    producer.send(data)
  }
}