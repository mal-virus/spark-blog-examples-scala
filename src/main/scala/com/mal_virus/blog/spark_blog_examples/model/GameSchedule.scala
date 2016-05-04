package com.mal_virus.blog.spark_blog_examples.model

case class GameSchedule
    (gameDate: String, gameNumber: String, dayOfWeek: String, visitingTeam: String, visitingLeague: String,
    visitorGameNum: String, homeTeam: String, homeLeague: String, homeGameNum: String,
    timeOfDay: String, postponed: String, makeupDate: String) {
  override def toString = {
    "game date " + gameDate + " game number " + gameNumber + " days of week " +
				dayOfWeek + " visiting team " + visitingTeam + " visiting league " +
				visitingLeague + " visitor game " + visitorGameNum + " home team " +
				homeTeam + " home league " + homeLeague + " home game num " +
				homeGameNum + " time of day " + timeOfDay + " postponed " + postponed +
				" makeup date " + makeupDate
  }
}

object GameSchedule {
  val serialVersionUID = -5767263494876444456L;
  val columns = List("game_date","visiting_team","home_team","day_of_week","game_number","home_game_num",
                    "home_league","makeup_date","postponed","time_of_day","visiting_league","visitor_game_num")
}