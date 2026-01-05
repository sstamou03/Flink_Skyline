package ECE613

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator
import org.apache.flink.util.Collector

import java.util.{ArrayList => JArrayList, List => JList}
import scala.collection.JavaConverters._


object Operators {

  class Operator1 extends KeyedProcessFunction[String, Point, Point] {

    //STATE
    private var skylineState: ValueState[JList[Point]] = _

    override def open(parameters: Configuration): Unit = {
      val descriptor = new ValueStateDescriptor[JList[Point]](
        "skylineState",
        classOf[JList[Point]]
      )
      skylineState = getRuntimeContext.getState(descriptor)
    }

    override def processElement(value: Point, ctx: KeyedProcessFunction[String, Point, Point]#Context, out: Collector[Point]): Unit = {
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

  class Operator2 extends KeyedCoProcessFunction[Int,Point, String, String] {

    private var skylineState: ValueState[JList[Point]] = _

    override def open(parameters: Configuration): Unit = {
      val descriptor = new ValueStateDescriptor[JList[Point]](
        "globalState",
        classOf[JList[Point]]
      )
      skylineState = getRuntimeContext.getState(descriptor)
    }

    override def processElement1(value: Point, ctx: KeyedCoProcessFunction[Int, Point, String, String]#Context, out: Collector[String]): Unit = {
      var pointlistj = skylineState.value()
      if (pointlistj == null) {
        pointlistj = new JArrayList[Point]()
      }

      pointlistj.add(value)

      skylineState.update(pointlistj)
      /*
      val pointlist = pointlistj.asScala.toList //JArrayList -> Scala

      val newDpointList = value :: pointlist

      val localskyline = Dominance.localSkyline(newDpointList)

      localskyline match {

        case changedList if changedList != pointlist =>

          skylineState.update(new JArrayList(changedList.asJava))

        /*if (changedList.contains(value)) {
            out.collect(value)
          }*/

        case _ => ()
      }
       */
    }

    override def processElement2(value: String, ctx: KeyedCoProcessFunction[Int, Point, String, String]#Context, out: Collector[String]): Unit ={

      val pointlistj = skylineState.value()

      if (pointlistj == null){
        out.collect("The output of the skyline query is empty")
      }else {
        val pointlist = pointlistj.asScala.toList

        val skyline = Dominance.localSkyline(pointlist)

        skylineState.update(new JArrayList[Point](skyline.asJava))

        val output = skyline.map{

          case Point(_,List(y,z)) => s"$y , $z"

        }.mkString(" \n")

        val end = System.currentTimeMillis()


        val result = (
          "="*100 + "\n" +
          "The output of the query is :" + output + "\n"
          +"="*100
          +"\n"+ end
          )

        out.collect(result)




      }
    }
  }




}