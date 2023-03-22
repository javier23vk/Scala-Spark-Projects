

import org.apache.spark.sql._
import org.apache.log4j._


import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object TravelMain {


  case class Trip (Destination: String, StartDate: String, EndDate: String,  TravelerNationality: String)


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().master("local[*]").appName("MyRealState").getOrCreate()
    import spark.implicits._

    val data  = spark.read.option("header",value = true).csv("./resources/Travel details dataset.csv")
    val dataWithCorrectNames = data
      .withColumnRenamed("Start date", "StartDate")
      .withColumnRenamed("End date", "EndDate")
      .withColumnRenamed("Duration (days)", "Duration")
      .withColumnRenamed("Traveler name", "TravelerName")
      .withColumnRenamed("Traveler age", "TravelerAge")
      .withColumnRenamed("Traveler gender", "TravelerGender")
      .withColumnRenamed("Traveler nationality", "TravelerNationality")
      .withColumnRenamed("Accommodation type", "AccommodationType")
      .withColumnRenamed("Accommodation cost", "AccommodationCost")
      .withColumnRenamed("Transportation type", "Transportation Type")
      .withColumnRenamed("Transportation cost", "TransportationCost").as[Trip]


    val destinationSeparated = dataWithCorrectNames
      .withColumn("City", split($"Destination",",").getItem(0))
      .withColumn("Country", when(size(split($"Destination",","))>1,split($"Destination",",").getItem(1) ).otherwise(null) ).na.drop()

    val selectedData = destinationSeparated.select("City", "TravelerNationality").groupBy("City", "TravelerNationality" )
      .count().orderBy("City", "TravelerNationality")


    val win = Window.partitionBy("City").orderBy("City")

    val result = selectedData.withColumn("maxCount", max($"count").over(win))
      .filter($"maxCount" ===$"count").orderBy("City")


    val resGrouped = result.groupBy("City")
      .agg(collect_set("TravelerNationality").alias("TravelerNationality"))


    val percentages = selectedData
      .withColumn("Percentage(%)", max($"count"/sum("count").over(win)).over(win)*100 )
      .dropDuplicates("City")
      .select("City", "Percentage(%)").orderBy("City")


    val resUnited = resGrouped.join(percentages,usingColumn = "City")
    resUnited.show(50)
    spark.close()
  }
}

