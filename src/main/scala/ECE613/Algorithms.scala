package ECE613


//======================================================MR-GRID==========================================================================
// Data Point
case class Point(id: String, values: List[Double])

object Dominance {

  /* Logic of the dominance for skyline query*/

  def dominance (p1: Point, p2:Point): Boolean = {
    val pairs = p1.values.zip(p2.values)

    val weak = pairs.forall(pair => pair match {
      case (p1,p2) => p1 <= p2
    })


    val strict = pairs.exists(pair => pair match {
      case (v1, v2) => v1 < v2
    })

    weak && strict
  }

  def localSkyline(dpoints: List[Point]): List[Point] = {

    dpoints.filterNot{ point =>

      dpoints.exists(dp =>
        dp != point && dominance(dp,point)

      )
    }
  }
}


object Algorithms {

  def MRDim(point: Point, partionNum: Int, Vmax: Double) = {

    val partvalue = point.values.head // separate according to the first value
    val range = Vmax / partionNum
    val partId = (partvalue / range).toInt

    partId match {

      case id if id >= partionNum => partionNum - 1
      case _ => partId

    }
  }
}

//=======================================================================================================================================
