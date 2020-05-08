package com.spark.assignment1

case class Game(
  game_id: Long,
  season: Int,
  //type: String,
  date_time: String,
  date_time_GMT: String,
  away_team_id: Int,
  home_team_id: Int,
  away_goals: Int,
  home_goals: Int,
  outcome: String,
  home_rink_side_start: String,
  venue: String,
  venue_link: String,
  venue_time_zone_id: String,
  venue_time_zone_offset: Int,
  venue_time_zone_tz: String
               )
