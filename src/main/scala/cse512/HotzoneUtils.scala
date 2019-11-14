package cse512

object HotzoneUtils {

  def isPointInside(queryRectangle: String, pointString: String ): Boolean = {
    // YOU NEED TO CHANGE THIS PART
    return true // YOU NEED TO CHANGE THIS PART
  }

  // YOU NEED TO CHANGE THIS PART
  def isPointInside(rectangle: String, point: String): Boolean = {
    try{
      var rectanglePoints = new Array[String](4)
      var p1x = rectanglePoints(0).trim().toDouble
      var p1y = rectanglePoints(1).trim().toDouble
      var p2x = rectanglePoints(2).trim().toDouble
      var p2y = rectanglePoints(3).trim().toDouble

      var minX = 0.0
      var minY = 0.0
      var maxX = 0.0
      var maxY = 0.0

      if(p1x > p2x) {
        minX = p2x
        maxX = p1x
      }
      else{
        minX = p1x
        maxX = p2x
      }

      if(p1y > p2y){
        minY = p2y
        maxY = p1y
      }
      else{
        minY = p1y
        maxY = p2y
      }

      var testPoint = point.split(",")
      var x = testPoint(0).trim().toDouble
      var y = testPoint(1).trim().toDouble

      if(minX <= x && x <= maxX && minY <= y && y <= maxY){
        return true
      }
      return false
    }
    catch {
      case _: Throwable => return false
    }
  }
}
