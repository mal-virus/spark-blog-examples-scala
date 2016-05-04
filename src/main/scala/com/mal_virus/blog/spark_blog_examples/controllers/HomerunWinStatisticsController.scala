package com.mal_virus.blog.spark_blog_examples.controllers

import org.apache.spark.SparkContext
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.SparkConf

object HomerunWinStatisticsController {
  def manipulateDate(game: Array[String]) = {
    game(0) = game(0).substring(1,5)
    game
  }
  
	def main(args: Array[String]) {
		val conf = new SparkConf()
		  .setAppName("Blog Example: Statistics and Correlation")
	  val sc = new SparkContext(conf)
		/*
		val gameLogs = sc.textFile(args(0))
		val mappedFile = gameLogs.map {_.split(",") }
		val parsedDate = mappedFile map manipulateDate
		*/
		
		val parsedDate = sc.textFile(args(0)).map(_.split(","))
		parsedDate foreach manipulateDate
		
		val mappedByVisitingTeam = parsedDate.groupBy { line => (line(3) + "," + line(0)) }
		val mappedByHomeTeam = parsedDate.groupBy { line => (line(6) + "," + line(0)) }
		
		val joined = mappedByVisitingTeam join mappedByHomeTeam
		
		val mappedTo = joined map calculateByTeam
		val homeruns = mappedTo map(_._1)
		val winningPercentage = mappedTo map(_._2)
		
		val correlation = Statistics.corr(homeruns,winningPercentage,"pearson")
		println("**************Pearson coefficiant for homeruns to winning percentage " + correlation)
		
		val homerunStats = homeruns.stats
		// temporarily print out to console some example statistics that are
    // included in StatCounter - see Javadocs for complete list
    println("**************Mean of homeruns " + homerunStats.mean)
    println("**************Standard deviation of homeruns " + homerunStats.stdev)
    println("**************Variance of homeruns " + homerunStats.variance)

    val winningPercentageStats = winningPercentage.stats
    // temporarily print out to console some example statistics that are
    // included in StatCounter - see Javadocs for complete list
    println("**************Mean of winning percentage " + winningPercentageStats.mean)
    println("**************Standard deviation of winning percentage " + winningPercentageStats.stdev)
    println("**************Variance of winning percentage " + winningPercentageStats.variance)
	}

	def calculateByTeam(v1:  (String, (Iterable[Array[String]], Iterable[Array[String]]))) = {
		  var teamHomeRuns, teamWins, gamesPlayed = 0.0
		  var allGames =
		    v1._2._1.map { x => (x(9).toDouble,x(10).toDouble,x(25).toInt) } ++
		    v1._2._2.map { x => (x(10).toDouble,x(9).toDouble,x(53).toInt) }
		  
		  for( game <- allGames) {
		      if(game._1>game._2) teamWins = teamWins +1
		      gamesPlayed = gamesPlayed +1
		      teamHomeRuns = teamHomeRuns +game._3
		  }
		  (teamHomeRuns,teamWins/gamesPlayed)
		}
	
	def printBaseball() {
		val citation = """
		Regular Season Schedules
		The information used here was obtained free of
		charge from and is copyrighted by Retrosheet.  Interested
		parties may contact Retrosheet at 20 Sunset Rd., Newark, DE 19711.
		Available at: http://www.retrosheet.org/schedule/index.html
		"""
		println(citation)	
	}
}