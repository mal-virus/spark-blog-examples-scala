package com.mal_virus.blog.spark_blog_examples.controllers

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.mal_virus.blog.spark_blog_examples.model.GameSchedule
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.CassandraRDD

object BaseballDataControllerCassandra {
  def main(args: Array[String]) {
    val conf = new SparkConf()
		  .setAppName("Boston Red Sox Scheduled Day Games")
		  .set("spark.cassandra.connection.host", args(0))
	  val sc = new SparkContext(conf)
    
    val schedules = sc.cassandraTable[GameSchedule]("baseball_examples", "game_schedule")
    
		// Filter out away games played by the team being examined.
		val awayGames = schedules.filter(gameSchedule => gameSchedule.visitingTeam.equals(args(1)) && gameSchedule.timeOfDay.equals("d"))

		// Filter out home games played by the team being examined.
		val homeGames = schedules.filter(gameSchedule => gameSchedule.homeTeam.equals(args(1)) && gameSchedule.timeOfDay.equals("d"))

		// Save back to Cassandra
		awayGames.saveAsCassandraTable("baseball_examples", "away_day_games", SomeColumns.seqToSomeColumns(GameSchedule.columns))

		// Save back to Cassandra
		homeGames.saveAsCassandraTable("baseball_examples", "home_day_games", SomeColumns.seqToSomeColumns(GameSchedule.columns))
  }
}