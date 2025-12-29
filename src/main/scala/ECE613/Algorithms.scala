package ECE613


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

  //====================================================== MR - DIM ==========================================================================

  def MRDim(point: Point, partionNum: Int, Vmax: Double) = {

    val partvalue = point.values.head // separate according to the first value
    val range = Vmax / partionNum
    val partId = (partvalue / range).toInt

    partId match {

      case id if id >= partionNum => partionNum - 1
      case _ => partId

    }
  }

  //============================================================= MR - GRID =======================================================

  def MRGrid(point: Point, partitionNum: Int, Vmax: Double) = {

    // if something is wrong, just send everything to partition 0
    if (point.values.length < 2 || partitionNum <= 0 || Vmax <= 0.0) {
      0
    } else {

      // extract the two dimensions x,y from the point
      val x = point.values.head
      val y = point.values.tail.head

      // #cells per dimension. for P partitions, sqrt(P) cells for x, sqrt(P) cells for y
      val cellsPerDim = math.max(1, math.sqrt(partitionNum.toDouble).toInt)
      val cellSize = Vmax / cellsPerDim

      // raw cell indices
      val ixRaw = (x / cellSize).toInt
      val iyRaw = (y / cellSize).toInt

      // values EXACTLY at Vmax end up in the last cell
      val ix = if (ixRaw >= cellsPerDim) cellsPerDim - 1 else ixRaw
      val iy = if (iyRaw >= cellsPerDim) cellsPerDim - 1 else iyRaw

      // Linearize (ix, iy) into a single partition id
      ix * cellsPerDim + iy
    }
  }

  //============================================================= MR - ANGLE =======================================================

  def MRAngle(point: Point, partitionNum: Int) = {

    if (point.values.length < 2 || partitionNum <= 0) {
      0
    } else {

      val x = point.values.head
      val y = point.values.tail.head

      // angle in (-π, π]
      val theta = math.atan2(y, x)

      val thetaPos = if (theta < 0) 0.0 else theta

      val maxAngle = math.Pi / 2.0

      val sectorWidth = maxAngle / partitionNum.toDouble
      val id = (thetaPos / sectorWidth).toInt

      // clamp (safety for theta == 2π due to floating point)
      if (id >= partitionNum) partitionNum - 1 else id
    }
  }

}



