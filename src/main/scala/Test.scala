import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala._

object ConnectionTest {
  def main(args: Array[String]): Unit = {
    println("--- Start Connection's Test ---")

    val conf = new Configuration()
    conf.setString(RestOptions.BIND_PORT, "8081")
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)

    val kafkaSource = KafkaSource.builder[String]()
      .setBootstrapServers("localhost:9092")
      .setTopics("input-tuples")
      .setGroupId("test-group")
      .setStartingOffsets(OffsetsInitializer.latest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

    val stream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")

    stream.print()

    env.execute("Flink Connection Test")
  }
}