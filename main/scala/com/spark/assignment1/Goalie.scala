package com.spark.assignment1

case class Goalie(
  game_id: Int,
  payer_id: Int,
  team_id: Int,
  timeOnIce: Int,
  assists: Int,
  goals: Int,
  pim: Int,
  shots: Int,
  saves: Int,
  powerPlaySaves: Int,
  shortHandedSaves: Int,
  evenSaves: Int,
  shortHandedShotsAgainst: Int,
  evenShotsAgainst: Int,
  powerPlayShotsAgainst: Int,
  decision: String,
  savePercentage: String,
  powerPlaySavePercentage: String,
  evenStrengthSavePercentage: String
                 )
