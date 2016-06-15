package com.mal_virus.blog.spark_blog_examples.controllers

import com.mal_virus.blog.spark_blog_examples.Baseball

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrameStatFunctions
import org.apache.spark.util.StatCounter


object HomerunWinStatisticsDatasetController extends Baseball {
  def main(args: Array[String]) {
    if(args.length<1) {
      println("Please provide an input location")
      System.exit(0)
    }
    // print the source data
    printBaseball()
    
    // Setup our environment
    val conf = new SparkConf()
      .setAppName("Blog Example: Statistics and Correlation")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    // This import allows us to cast to Datasets[T] easily
    import sqlContext.implicits._
    
    // raw lines of our source file stored in a Dataset[String]
    val gameLogs = sqlContext.read.text(args(0)).as[String]
    
    // Rather than just split the lines by the comma delimiter,
    // we'll use those splits to create a new instance of a case class
    // When using a case class with Datasets, the SQL schema is automatically inferred
    val mappedFile = gameLogs map { game =>
      val splits = game.split(",")
      val year = splits(0).substring(1,5)
      val visitingTeam = splits(3)
      val homeTeam = splits(6)
      val visitingScore = splits(9)
      val homeScore = splits(10)
      val visitingHomeRuns = splits(25)
      val homeHomeRuns = splits(53)
      Game(year,visitingTeam, homeTeam, visitingScore.toInt, homeScore.toInt, visitingHomeRuns.toInt, homeHomeRuns.toInt)
    }
    
    // Grouped datasets don't have a relatively easy join method
    // Instead, we will iterate through each "game" once and return two objects
    // Each object contains:
    // 1. teamName+year
    // 2. Number of homeruns for that team
    // 3. an Int signifying if that team accrued a win
    val mappedByTeam = mappedFile.flatMap(calculateResults)
    
    // Group the data by each team per season
    val joined = mappedByTeam.groupBy(_.key)
    
    // Finally, we collect all the data per team per season
    // into one object containing the information we need
    // We don't need the key listed below, but mapGroups takes a function of (key,group)=>T
    val mappedTo = joined.mapGroups((key, results) => consolidate(results))  
    
    // This function takes a dataframe and two rows to return the correlation as a double
    val correlation = mappedTo.toDF.stat.corr("homeRuns","winningPercentage","pearson")
    println("**************Pearson coefficient for homeruns to winning percentage " + correlation)
    
    // If we want, we can turn our Dataset back to an RDD
    val homeruns = mappedTo map(_.homeRuns) rdd
    val winningPercentage = mappedTo map(_.winningPercentage) rdd
    
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
  
  /**
   * This function will parse the Result of the Game for each team
   * A result has the key as teamName+season,the homeruns for this game, and an Int signifying if the team won
   */
  def calculateResults(game: Game) = {
    val visitorKey = game.visitingTeam + game.year
    val homeKey = game.homeTeam + game.year
    var visitingWin, homeWin = 0
    if(game.visitingScore>game.homeScore) visitingWin = 1
    if(game.homeScore>game.visitingScore) homeWin = 1
    Seq(Result(visitorKey,game.visitingHomeRuns,visitingWin),Result(homeKey,game.homeHomeRuns,homeWin))
  }
  
  /**
   * Takes all Results and collects them into one season
   */
  def consolidate(results: Iterator[Result]) = {
	  val list = results.toList
	  val games = list.size
	  val wins = list.map(_.win).sum 
	  val winningPercentage = wins.toDouble/games.toDouble 
	  Season(
	    list.map(_.homeruns).sum,
	    games,
	    wins,
	    winningPercentage)
	}
  
  /**
   * This is the minimal version of our main method
   */
  def trimmedMain(args: Array[String]) {
    if(args.length<1) {
      println("Please provide an input location")
      System.exit(0)
    }
    // Setup our environment
    val sqlContext = new SQLContext(new SparkContext(new SparkConf()
      .setAppName("Blog Example: Statistics and Correlation")))
    import sqlContext.implicits._
    
    // Create our input and transform our data
    val mappedTo = sqlContext.read.text(args(0)).as[String]
      .map { game =>
          val splits = game.split(",")
          Game(splits(0).substring(1,5),splits(3), splits(6), splits(9).toInt, splits(10).toInt, splits(25).toInt, splits(53).toInt)
        }
      .flatMap(calculateResults)
      .groupBy(_.key)
      .mapGroups((key, results) => consolidate(results))  
    
    
    // Run our analysis
    val correlation = mappedTo.toDF.stat.corr("homeRuns","winningPercentage","pearson")
    println("**************Pearson coefficient for homeruns to winning percentage " + correlation)
    
    // Print
    def printStatistics(n: String, s: StatCounter) {
      printf("**************Mean of %4$s: %f\n**************Standard deviation of %4$s: %f\n" +
             "**************Variance of %4$s: %f\n",s.mean,s.stdev,s.variance, n)
    }
    printStatistics("homeruns",mappedTo.map(_.homeRuns).rdd.stats)
    printStatistics("winning percentage",mappedTo.map(_.winningPercentage).rdd.stats)
  }
}
case class Game(year: String, visitingTeam: String, homeTeam: String, visitingScore: Int, homeScore: Int, visitingHomeRuns: Int, homeHomeRuns: Int)
case class Result(key: String, homeruns: Int, win: Int)
case class Season(homeRuns: Double, games: Double, wins: Double, winningPercentage: Double)
