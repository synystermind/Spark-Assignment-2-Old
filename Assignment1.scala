package com.spark.assignment1

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

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



  /** Problem 1
    * How many games were played in each season by all teams combined
    */
  def problem1(game: RDD[Game]): Long = {
    game.distinct().count()
  }

  /** Problem 1 SQL
    * select distinct
    *   count(g.game_id)
    *   ,g.season
    * from game g;
    */

  /** Problem 2
    * How Many Players Played in the NHL Each Season?
    */
  def problem2(skaterStats: RDD[SkaterStats]): Long = {
   skaterStats.map(skater => (skater.player_id, skater)).count() }
/**    val mappedgames = game.map(game => (game.game_id, game))
   mappedgames.join(mappedskaters).count()
  }**/
  /** Problem 2 SQL
    * select distinct
    *   count(ss.player_id)
    *   ,g.season
    * from skaterStats ss
    * inner join game g on g.game_id = ss.game_id;
    */

  /** Problem 3
    * What Player played for the most teams in the time-span of this data?
    */
  def problem3(skaterStats: RDD[SkaterStats], playerInfo: RDD[PlayerInfo]): Seq[String]  = {
    val mappedskaters = skaterStats.map(skater => (skater.team_id, skater))
    val mappedplayers = playerInfo.map(player => (player.player_id, player))
    mappedskaters.leftOuterJoin(mappedplayers).collect()
  }
  /** Problem 3 SQL
    * select
    * pi.player_id
    * ,pi.firstName
    * ,pi.lastName
    * ,ti.shortName
    * ,ti.teamName
    * from player_info pi
    * left join game_skater_stats ss on ss.game_id=pi.game_id AND ss.player_id=pi.player_id
    * left join team_info ti on ti.team_id=ss.team_id
    * order by pi.player_id, ti.teamName
    */


  /** Problem 4
    * Who scored the most points as a defenseman over his career?
    */
  def problem4(skaterStats: RDD[SkaterStats], playerInfo: RDD[PlayerInfo]): Seq[String] = {
    val mappedplayers = playerInfo.map(player => (player.primaryPosition, player).equals("D"))
    val mappedskaters = skaterStats.map(skater => (skater.team_id, skater))
    mappedskaters.leftOuterJoin(mappedplayers).collect()
  }
  /** Problem 4 & 5 SQL
    * select
    * * pi.player_id
    * * ,pi.firstName
    * * ,pi.lastName
    * * ,ti.shortName
    * * ,ti.teamName
    * * from player_info pi
    * * left join game_skater_stats ss on ss.game_id=pi.game_id AND ss.player_id=pi.player_id
    * * left join team_info ti on ti.team_id=ss.team_id
    * * where primaryPosition = 'D'
    * * order by pi.player_id, ti.teamName
    */


  /** Problem 5
    * What team(s) did that defenseman play for over his career?
    */
  def problem5(skaterStats: RDD[SkaterStats], playerInfo: RDD[PlayerInfo], teamInfo: RDD[TeamInfo]): Seq[String] = {
    val mappedplayers = playerInfo.map(player => (player.primaryPosition, player).equals("D"))
    val mappedskaters = skaterStats.map(skater => (skater.team_id, skater))
    val mappedteams = teamInfo.map(team => (team.team_id, team))
    mappedskaters.leftOuterJoin(mappedplayers)
    mappedskaters.leftOuterJoin(mappedteams).collect()
  }


  // Helper function to parse the timestamp format used in the trip dataset.
  private def parseTimestamp(timestamp: String) = LocalDateTime.from(timestampFormat.parse(timestamp))
}

