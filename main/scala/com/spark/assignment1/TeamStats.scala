package com.spark.assignment1

case class TeamStats(
  game_id: Int,
  team_id: Int,
  HoA: String,
  won: Boolean,
  settled_in: String,
  head_coach: String,
  goals: Int,
  shots: Int,
  hits: Int,
  pim: Int,
  powerPlayOpportunities: Int,
  powerPlayGoals: Int,
  faceOffWinPercentage: Double,
  giveaways: Int,
  takeaways: Int
)
