package ECE613

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import java.util.{ArrayList => JArrayList, List => JList}
import scala.collection.JavaConverters._

object Operators {

  class Operator1 extends KeyedProcessFunction[Int, Point, Point] {

    //STATE
    private var skylineState: ValueState[JList[Point]] = _

    override def open(parameters: Configuration): Unit = {
      val descriptor = new ValueStateDescriptor[JList[Point]](
        "skylineState",
        classOf[JList[Point]]
      )
      skylineState = getRuntimeContext.getState(descriptor)
    }

    override def processElement(value: Point, ctx: KeyedProcessFunction[Int, Point, Point]#Context, out: Collector[Point]): Unit = {
      var pointlistj = skylineState.value()
      if (pointlistj == null) {
        pointlistj = new JArrayList[Point]()
      }
      val pointlist = pointlistj.asScala.toList //JArrayList -> Scala

      val newDpointList = value :: pointlist

      val localskyline = Dominance.localSkyline(newDpointList)

      localskyline match {

        case changedList if changedList != pointlist =>

          skylineState.update(new JArrayList(changedList.asJava))

          if (changedList.contains(value)) {
            out.collect(value)
          }

        case _ => ()
      }

    }

  }
}