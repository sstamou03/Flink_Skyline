package ECE613

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties
//import scala.collection.immutable.IntMap.Nil.fold
import scala.util.Random

object GeneratorTest {

  def main(args: Array[String]): Unit = {

    val broker = "localhost:9092"
    val topic = "Input"

    val prop = new Properties()
    prop.put("bootstrap.servers", broker)
    prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](prop)
    val random = new Random()

    (1 to 1000000).view.map{ _ =>   //view -> lazy, we avoid memory errors

      val x = random.nextDouble()*1000
      val y = random.nextDouble()*1000

      (x,y)

    }.foreach{case (x,y) =>

      val msg = x+","+y
      val record = new ProducerRecord[String, String](topic, msg)
      producer.send(record)
      println(msg)

    }

    producer.close()
  }
}