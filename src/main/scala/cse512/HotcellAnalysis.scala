package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  // YOU NEED TO CHANGE THIS PART

  import spark.implicits._
  pickupInfo.createOrReplaceTempView("pickupInfo")
  spark.udf.register("isPointInside",(x:Int, y:Int, minX: Int, minY: Int, maxX: Int, maxY: Int) => HotcellUtils.isPointInside(x, y, minX, minY, maxX, maxY))
  val query = "select x,y,z from pickupInfo where isPointInside(x,y,%d,%d,%d,%d)".format(minX.toInt,minY.toInt,maxX.toInt,maxY.toInt)
  pickupInfo = spark.sql(query)
  pickupInfo.createOrReplaceTempView("pickupInfo")
  pickupInfo = spark.sql("select x,y,z,count(*) as count from pickupInfo GROUP BY x,y,z")

  var discreteCoordinates: Map[String,Long] = Map()
  var homogenousPoints:scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map()
  var coordinateX = scala.collection.mutable.Set[Int]()
  var coordinateY  = scala.collection.mutable.Set[Int]()
  var coordinateZ  = scala.collection.mutable.Set[Int]()

  var power2 = 0l
  var cellCount = 0l

  for (point <- pickupInfo.collect()) {
    var cellCount = point.getLong(3)
    var x = point.getInt(0)
    var y = point.getInt(1)
    var z = point.getInt(2)
    var dataKey = "%d%d%d".format(x,y,z)
    discreteCoordinates += (dataKey -> cellCount)
    power2 += cellCount * cellCount
    cellCount += cellCount
    homogenousPoints.update("%d%d%d".format(x-minX.toInt, y-minY.toInt, z-minZ), dataKey)
    coordinateX += x
    coordinateY += y
    coordinateZ += z
  }

  val X = cellCount.toDouble / numCells.toDouble
  val S = Math.sqrt(power2.toDouble / numCells.toDouble - X * X)

  var result:Map[(Int,Int,Int),Double] = Map()
  var xMin = minX.toInt
  var yMin = minY.toInt
  var zMin = minZ.toInt

  for (i <- coordinateX) {
    for(j <- coordinateY) {
      for(k <- coordinateZ) {
        val (wijxj, wij) = getWeights(homogenousPoints, discreteCoordinates,i-xMin,j-yMin,k-zMin)
        result += (i,j,k) -> (wijxj - (wij * X)) / (S * math.sqrt((numCells * wij - (wij * wij)) / (numCells - 1)))
      }
    }
  }

  var sortedResult = result.toSeq.sortBy(- _._2)
  if (sortedResult.size > 50) {
    sortedResult = sortedResult.take(50)
  }
  var mapResult = sortedResult.map{ case (k,v) => k}
  return mapResult.toDF().coalesce(1)
}

  def getWeights(homogenousPoints:scala.collection.mutable.Map[String,String], pointMap : Map[String,Long], coordinateX: Int, coordinateY: Int, coordinateZ: Int) : (Long , Int) = {
    var total = 0.toLong
    var neighboringCount = 0
    for (x <- -1 to 1) {
      for(y <- -1 to 1) {
        for(z <- -1 to 1) {
          var temp = 0l
          if (homogenousPoints.contains("%d%d%d".format(coordinateX + z, coordinateY + y, coordinateZ + x))) {
            temp = pointMap(homogenousPoints("%d%d%d".format(coordinateX + z, coordinateY + y, coordinateZ + x)))
          }
          total += temp
          neighboringCount += 1
        }
      }
    }
    (total, neighboringCount)
  }
}
