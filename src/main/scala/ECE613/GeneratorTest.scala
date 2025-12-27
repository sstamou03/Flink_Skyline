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

   // val distro = "uniform"
   // val distro = "anticorrelated"
    val distro = "correlated"

    println(distro)

    (1 to 15000000).view.map{ _ => //view -> lazy, we avoid memory errors

      distro match {
        case "correlated" => val x = random.nextDouble() * 1000
          val y = x + (random.nextGaussian() * 50)
          (x, y)

        case "anticorrelated" => val x = random.nextDouble() * 1000
          val y =  (1000 - x) + (random.nextGaussian() * 50)
          (x,y)

        case _ => val x = random.nextDouble()*1000
          val y = random.nextDouble()*1000
          (x,y)
      }

    }.foreach{case (x,y) =>

      val msg = x+","+y
      val record = new ProducerRecord[String, String](topic, msg)
      producer.send(record)
      println(msg)

    }

    producer.close()
  }
}