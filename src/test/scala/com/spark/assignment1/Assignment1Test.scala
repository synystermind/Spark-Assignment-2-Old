package com.spark.assignment1

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers
import org.apache.spark.sql.types._
import scala.concurrent.duration._

class Assignment1Test extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  /**
    * Set this value to 'true' to halt after execution so you can view the Spark UI at localhost:4040.
    * NOTE: If you use this, you must terminate your test manually.
    * OTHER NOTE: You should only use this if you run a test individually.
    */
  val BLOCK_ON_COMPLETION = false;

  // Paths to your data.
  val GAME_CSV_PATH = "data/game.csv" //Game
  val GAME_GOALIE_STATS_CSV_PATH = "data/game_goalie_stats.csv" //Goalie
  val GAME_PLAYS_CSV_PATH = "data/game_plays.csv" //Plays
  val GAME_PLAYS_PLAYERS_CSV_PATH = "data/game_plays_players.csv" //PlayedBy
  val GAME_SHIFTS_CSV_PATH = "data/game_shifts.csv" //Shifts
  val GAME_SKATER_STATS_CSV_PATH = "data/game_skater_stats.csv" //SkaterStats
  val GAME_TEAM_STATS_CSV_PATH = "data/game_team_stats.csv" //TeamStats
  val PLAYER_INFO_CSV_PATH = "data/player_info.csv" //PlayerInfo
  val TEAM_INFO_CSV_PATH = "data/team_info.csv" //TeamInfo

  /**
    * Create a SparkSession that runs locally on our laptop.
    */
  val spark =
    SparkSession
      .builder()
      .appName("Assignment 1")
      .master("local[*]") // Spark runs in 'local' mode using all cores
      .getOrCreate()

  /**
    * Encoders to assist converting a csv records into Case Classes.
    * They are 'implicit', meaning they will be picked up by implicit arguments,
    * which are hidden from view but automatically applied.
    */
  implicit val gameEncoder: Encoder[Game] = Encoders.product[Game]
  implicit val goalieEncoder: Encoder[Goalie] = Encoders.product[Goalie]
  implicit val playsEncoder: Encoder[Plays] = Encoders.product[Plays]
  implicit val playedByEncoder: Encoder[PlayedBy] = Encoders.product[PlayedBy]
  implicit val shiftsEncoder: Encoder[Shifts] = Encoders.product[Shifts]
  implicit val skaterStatsEncoder: Encoder[SkaterStats] = Encoders.product[SkaterStats]
  implicit val teamStatsEncoder: Encoder[TeamStats] = Encoders.product[TeamStats]
  implicit val playerInfoEncoder: Encoder[PlayerInfo] = Encoders.product[PlayerInfo]
  implicit val teamInfoEncoder: Encoder[TeamInfo] = Encoders.product[TeamInfo]

  /**
    * Let Spark infer the data types. Tell Spark this CSV has a header line.
    */
  val csvReadOptions =
    Map("inferSchema" -> true.toString, "header" -> true.toString)

  /**
    * Create Game Spark collections + Write to Parquet
    */
  def gameDataDS: Dataset[Game] = spark.read.options(csvReadOptions).csv(GAME_CSV_PATH).as[Game]
  def gameDataDF: DataFrame = gameDataDS.toDF()
  def gameDataRdd: RDD[Game] = gameDataDS.rdd
  gameDataDF.write.parquet("data/game_to_parquet")
  def game_parquetDF: DataFrame= spark.read.parquet("data/game_to_parquet")


  /**
    * Create Goalie Spark collections Write to Parquet
    */
  def goalieDataDS: Dataset[Goalie] = spark.read.options(csvReadOptions).csv(GAME_GOALIE_STATS_CSV_PATH).as[Goalie]
  def goalieDataDF: DataFrame = goalieDataDS.toDF()
  def goalieDataRdd: RDD[Goalie] = goalieDataDS.rdd
  goalieDataDF.write.parquet("data/goalie_to_parquet")
  def goalie_parquetDF: DataFrame= spark.read.parquet("data/goalie_to_parquet")

  /**
    * Create Plays Spark collections Write to Parquet
    */
  def playsDataDS: Dataset[Plays] = spark.read.options(csvReadOptions).csv(GAME_PLAYS_CSV_PATH).as[Plays]
  def playsDataDF: DataFrame = playsDataDS.toDF()
  def playsDataRdd: RDD[Plays] = playsDataDS.rdd
  playsDataDF.write.parquet("data/plays_to_parquet")
  def plays_parquetDF: DataFrame= spark.read.parquet("data/plays_to_parquet")

  /**
    * Create Played By Spark collections Write to Parquet
    */
  def playedByDataDS: Dataset[PlayedBy] = spark.read.options(csvReadOptions).csv(GAME_PLAYS_PLAYERS_CSV_PATH).as[PlayedBy]
  def playedByDataDF: DataFrame = playedByDataDS.toDF()
  def playedByDataRdd: RDD[PlayedBy] = playedByDataDS.rdd
  playedByDataDF.write.parquet("data/playedBy_to_parquet")
  def playedBy_parquetDF: DataFrame= spark.read.parquet("data/playedBy_to_parquet")

  /**
    * Create Shifts Spark collections Write to Parquet
    */
  def shiftsDataDS: Dataset[Shifts] = spark.read.options(csvReadOptions).csv(GAME_SHIFTS_CSV_PATH).as[Shifts]
  def shiftsDataDF: DataFrame = shiftsDataDS.toDF()
  def shiftsDataRdd: RDD[Shifts] = shiftsDataDS.rdd
  shiftsDataDF.write.parquet("data/shifts_to_parquet")
  def shifts_parquetDF: DataFrame= spark.read.parquet("data/shifts_to_parquet")

  /**
    * Create Skater Stats Spark collections Write to Parquet
    */
  def skaterStatsDataDS: Dataset[SkaterStats] = spark.read.options(csvReadOptions).csv(GAME_SKATER_STATS_CSV_PATH).as[SkaterStats]
  def skaterStatsDataDF: DataFrame = skaterStatsDataDS.toDF()
  def skaterStatsDataRdd: RDD[SkaterStats] = skaterStatsDataDS.rdd
  skaterStatsDataDF.write.parquet("data/skaterStats_to_parquet")
  def skaterStats_parquetDF: DataFrame= spark.read.parquet("data/skaterStats_to_parquet")

  /**
    * Create Team Stats Spark collections Write to Parquet
    */
  def teamStatsDataDS: Dataset[TeamStats] = spark.read.options(csvReadOptions).csv(GAME_TEAM_STATS_CSV_PATH).as[TeamStats]
  def teamStatsDataDF: DataFrame = teamStatsDataDS.toDF()
  def teamStatsDataRdd: RDD[TeamStats] = teamStatsDataDS.rdd
  teamStatsDataDF.write.parquet("data/teamStats_to_parquet")
  def teamStats_parquetDF: DataFrame= spark.read.parquet("data/teamStats_to_parquet")

  /**
    * Create Player Info Spark collections Write to Parquet
    */
  def playerInfoDataDS: Dataset[PlayerInfo] = spark.read.options(csvReadOptions).csv(PLAYER_INFO_CSV_PATH).as[PlayerInfo]
  def playerInfoDataDF: DataFrame = playerInfoDataDS.toDF()
  def playerInfoDataRdd: RDD[PlayerInfo] = playerInfoDataDS.rdd
  playerInfoDataDF.write.parquet("data/playerInfo_to_parquet")
  def playerInfo_parquetDF: DataFrame= spark.read.parquet("data/playerInfo_to_parquet")

  /**
    * Create Team Info Spark collections Write to Parquet
    */
  def teamInfoDataDS: Dataset[TeamInfo] = spark.read.options(csvReadOptions).csv(TEAM_INFO_CSV_PATH).as[TeamInfo]
  def teamInfoDataDF: DataFrame = teamInfoDataDS.toDF()
  def teamInfoDataRdd: RDD[TeamInfo] = teamInfoDataDS.rdd
  teamInfoDataDF.write.parquet("data/teamInfo_to_parquet")
  def teamInfo_parquetDF: DataFrame= spark.read.parquet("data/teamInfo_to_parquet")

  /**
    * Keep the Spark Context running so the Spark UI can be viewed after the test has completed.
    * This is enabled by setting `BLOCK_ON_COMPLETION = true` above.
    */
  override def afterEach: Unit = {
    if (BLOCK_ON_COMPLETION) {
      // open SparkUI at http://localhost:4040
      Thread.sleep(5.minutes.toMillis)
    }
  }

  /**
    * How many games were played in each season by all teams combined?
    */
  test("How many games were played in each season by all teams combined?") {
    Assignment1.problem1(gameDataRdd) must equal(11434)
  }

  /**
    * How Many Players Played in the NHL Each Season?
    */
 test("How Many Players Played in the NHL Each Season?") {
    Assignment1.problem2(skaterStatsDataRdd) must equal(411578)
  }

  /**
    * What Player played for the most teams in the timespan of this data?
    */
  test("What Player played for the most teams in the timespan of this data?") {
    Assignment1.problem3(playerInfoDataRdd) must equal(("Lee"))
  }

  /**
    * Who scored the most points as a defenseman over his career?
    */
  test("Who scored the most points as a defenseman over his career?") {
    Assignment1.problem4(playerInfoDataRdd) must equal(("Burns"))
  }

  /**
    * What team(s) did that defenseman play for over his career?
    */
  test("What team(s) did that defenseman play for over his career?") {
    Assignment1.problem5(playerInfoDataRdd) must equal(("MIN", "SJS"))
  }




}


