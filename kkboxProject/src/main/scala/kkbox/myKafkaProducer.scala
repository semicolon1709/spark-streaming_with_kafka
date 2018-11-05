package kkbox

import java.util.Properties
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

class myKafkaProducer {
  val props = new Properties()
  props.put("bootstrap.servers", "master:9092, slaver1:9092, slaver2:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("retries", "0");
  props.put("batch.size", "16384");
  props.put("linger.ms", "20");
  props.put("buffer.memory", "33554432");
  props.put("acks", "1")
  val producer = new KafkaProducer[String, String](props)

  def sendToKafkaAsync(topic: String, key: String, value:String): Unit= {
    val callback = new Callback {
      override def onCompletion(recordMetadata: RecordMetadata, e: Exception) = {
        if (e != null) {
          println("Ooops! " + e.printStackTrace())
        }
      }
    }

    producer.send(new ProducerRecord[String, String](topic, key, value), callback)
  }

  def sendToKafkaAsync(topic: String, value:String): Unit= {
    val callback = new Callback {
      override def onCompletion(recordMetadata: RecordMetadata, e: Exception) = {
        if (e != null) {
          println("Ooops! " + e.printStackTrace())
        }
      }
    }

    producer.send(new ProducerRecord[String, String](topic, value), callback)
  }
}



