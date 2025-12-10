package ECE613

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import scala.util.Random

object Job {

  def main(args: Array[String]): Unit = {

    val param = ParameterTool.fromArgs(args)
    val parallelism = param.getInt("parallelism", 4)
    val inputTopic = param.get("topic", "Input")
    val brokers = param.get("bootstrap.servers", "localhost:9092")
    val Vmax = param.getDouble("vmax", 1000.0) //Vmax MRDim
    val algorithm = param.get("MR", "dim")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(parallelism)
    env.getConfig.setGlobalJobParameters(param)

    val source = KafkaSource.builder[String]()
      .setBootstrapServers(brokers)
      .setTopics(inputTopic)
      .setGroupId("group_" + algorithm + "_" + System.currentTimeMillis())
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

    val input = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Input")

    //input.print()
    //val random = new Random()

    val points = input.map{ line =>
      try{

        val value = line.split(",").map(_.trim.toDouble).toList
        val random = new scala.util.Random()
        val id = random.nextInt().toString

        Point(id,value)
      }catch {
        case _ : Exception => Point("",List.empty)
      }
     }.filter(point => point.id != "" && point.values != List.empty)

    val partition = algorithm match {

      case "dim" => points.keyBy(point => Algorithms.MRDim(point,parallelism,Vmax))

      //TODO
      //case 2os

      //case 3os
    }

    val local = partition.process(new Operators.Operator1).name("local")
    val global = local.keyBy(_ => 1).process(new Operators.Operator1)

    global.map(p => s"*** RESULT: ${p.values.mkString(", ")}").print()

    env.execute("PLH613")

  }

}