package com.mal_virus.blog.spark_blog_examples.controllers

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object BaseballDataController {
  def appendGame(game: Array[String]) = game.reduce((x,y) => x + "," + y)
  
  def main(args: Array[String]) {
    val conf = new SparkConf()
		  .setAppName("Boston Red Sox Scheduled Day Games")
	  val sc = new SparkContext(conf)
    
    val schedules = sc.textFile(args(0))
    val mappedFile = schedules map {_.split(",")}
    
    // Filter out away games played by the team being examined.
		val awayGames = mappedFile.filter(line => line(3).equals(args(2)) && line(9).equals("d"))
		//Map array back to a String
    val mappedAwayGames = awayGames.map(appendGame)
    
    // Filter out home games played by the team being examined.
		val homeGames = mappedFile.filter(line => line(6).equals(args(2)) && line(9).equals("d"));
		//Map array back to a String
		val mappedHomeGames = homeGames.map(appendGame)

		// Save back to HDFS
		mappedAwayGames.saveAsTextFile(args(1) + "/awayGames");
		// Save back to HDFS
		mappedHomeGames.saveAsTextFile(args(1) + "/homeGames");
  }
}