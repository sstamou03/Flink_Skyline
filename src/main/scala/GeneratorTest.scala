import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.util.Random

object DataGenerator {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    val topic = "input-tuples"
    val random = new Random()

    println("--- Start the data generation ---")

    for (i <- 1 to 100000) {
      val x = random.nextInt(1000)
      val y = random.nextInt(1000)
      val message = s"$x,$y"

      val record = new ProducerRecord[String, String](topic, message)
      producer.send(record)

      if (i % 1000 == 0) Thread.sleep(100)
    }

    producer.close()
    println("--- End of Generation ---")
  }
}