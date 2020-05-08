package com.spark.assignment1

case class Plays(
  play_id: String,
  game_id: Int,
  play_num: Int,
  team_id_for: String,
  team_id_against: String,
  event: String,
  secondaryType: String,
  x: String,
  y: String,
  period: Int,
  periodType: String,
  periodTime: Int,
  periodTimeRemaining: Int,
  dateTime: String,
  goals_away: Int,
  goals_home: Int,
  description: String,
  st_x: String,
  st_y: String,
  rink_side: String
                )
