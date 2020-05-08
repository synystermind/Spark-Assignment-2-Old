package com.spark.assignment1

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.functions._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object Assignment1 {

  private val timestampFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("M/d/yyyy H:mm")

  /**
    * Helper function to print out the contents of an RDD
    * @param label Label for easy searching in logs
    * @param theRdd The RDD to be printed
    * @param limit Number of elements to print
    */
  private def printRdd[_](label: String, theRdd: RDD[_], limit: Integer = 20) = {
    val limitedSizeRdd = theRdd.take(limit)
    println(s"""$label ${limitedSizeRdd.toList.mkString(",")}""")
  }


  /**
    * How many games were played in each season by all teams combined
    */
  def problem1(game: RDD[Game]): Long = {
    game.map(x => x.game_id).count()
  }

  /** Problem 1 SQL
    * select distinct
    *   count(g.game_id)
    *   ,g.season
    * from game g;
    */

  /**
    * How Many Players Played in the NHL Each Season?
    */
  def problem2(skaterStats: RDD[SkaterStats]): Long = {
    skaterStats.map(x => x.player_id).count()
  }
  /** Problem 2 SQL
    * select distinct
    *   count(ss.player_id)
    *   ,g.season
    * from skaterStats ss
    * inner join game g on g.game_id = ss.game_id;
    */

  /**
    * What Player played for the most teams in the time-span of this data?
    */
  def problem3(trips: RDD[Trip]): Seq[String] = {
  //  trips.map(x => x.subscriber_type).collect()
  }

  /**
    * Who scored the most points as a defenseman over his career?
    */
  def problem4(trips: RDD[Trip]): String = {
  //  trips.filter(x => x.zip_code.equals(x.zip_code))
  }


  /**
    * What team(s) did that defenseman play for over his career?
    */
  def problem5(trips: RDD[Trip]): Long = {
  //  trips.filter(x =>  x.start_date < x.end_date).count()

  }


  /**
    * Did the 2012-2013 NHL lockout influence the 2013-2014 season when compared to the 2011-2012 season?
    */
  def problem6(trips: RDD[Trip]): Long = {
 //   trips.count()
  }

  // Helper function to parse the timestamp format used in the trip dataset.
  private def parseTimestamp(timestamp: String) = LocalDateTime.from(timestampFormat.parse(timestamp))
}


/**


/**
  * What percentage of people keep their bikes overnight at least on night?
  */
  def problem7(trips: RDD[Trip]): Double = {
    ???
  }
  /**
  * Ope! The docks were miscalibrated and only counted half of a trip duration. Double the duration of each trip so
  * we can have an accurate measurement.
  */
  def problem8(trips: RDD[Trip]): Double = {
    ???
  }

  /**
  * Find the coordinates (latitude and longitude) of the trip with the id 913401.
  */
  def problem9(trips: RDD[Trip], stations: RDD[Station]): (Double, Double) = {
???
  }

  def problem10(trips: RDD[Trip], stations: RDD[Station]): Array[(String, Long)] = {
    ???
  }

  /*
   Dataframes
  */
  /**
  * Select the 'trip_id' column
  */
  def dfProblem11(trips: DataFrame): DataFrame = {
    trips.filter(col("trip_id")).show()
  }
  /**
  * Count all the trips starting at 'Harry Bridges Plaza (Ferry Building)'
  */
  def dfProblem12(trips: DataFrame): DataFrame = {
    trips.select("station_id").show("Harry Bridges Plaza (Ferry Building)")
  }
  /**
  * Sum the duration of all trips
  */
  def dfProblem13(trips: DataFrame): Long = {
    trips.agg(sum("duration")).show(1)
  }
  **/
