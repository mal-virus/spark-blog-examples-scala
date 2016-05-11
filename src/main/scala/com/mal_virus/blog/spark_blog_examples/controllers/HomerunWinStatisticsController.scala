package com.mal_virus.blog.spark_blog_examples.controllers

import org.apache.spark.SparkContext
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.SparkConf
import com.mal_virus.blog.spark_blog_examples.Baseball

object HomerunWinStatisticsController extends Baseball {
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
    
    // raw lines of our source file stored in a Resilient Distributed Dataset
    val gameLogs = sc.textFile(args(0))
    
    // Break up the lines based on the comma delimiter
    val mappedFile = gameLogs.map(_.split(","))
    
    // Group by the visiting team and year played in
    val mappedByVisitingTeam = mappedFile.groupBy { line => (line(3) + "," + line(0).substring(1,5)) }
    // Group by the home team and year played in
    val mappedByHomeTeam = mappedFile.groupBy { line => (line(6) + "," + line(0).substring(1,5)) }
    // Since our data always comes in "YYYYMMDD" format, we'll skip the manipulateDate function
    // by using a substring
    
    // Join the visiting team and home team RDD together to get a whole
    // season of game logs for each team
    val joined = mappedByVisitingTeam join mappedByHomeTeam
    
    // Using the map function to transform the data into
    // a sum of homeruns and the winning percentage per team per season 
    val mappedTo = joined map calculateByTeam
    
    // Retrieve the values we care about as their own RDD
    val homeruns = mappedTo map(_._1)
    val winningPercentage = mappedTo map(_._2)
    
    // This function takes two RDD of doubles and a correlation type as String to return the correlation as a double
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

  
  /**
   * This function will parse out and calculate the total number of homeruns,
   * games won and games played for a team in a single season. Once this is
   * calculated the homerun total and winning percentage is returned.
   * Tuple is made up Tuple2<teamKey, Tuple2<gamesAsVisitor, gamesAsHome>>
   * It is similar to a map with two values
   */
  def calculateByTeam(v1:  (String, (Iterable[Array[String]], Iterable[Array[String]]))) = {
      var teamHomeRuns, teamWins, gamesPlayed = 0.0
      /**
       * v1._2._1 are all games played by our team as visitors
       * v1._2._2 are all games played by our team as the home team
       * allGames is a Tuple3 of (our team's score, opposition team's score, our team's homeruns)
       */
      var allGames =
        v1._2._1.map { x => (x(9).toDouble,x(10).toDouble,x(25).toInt) } ++
        v1._2._2.map { x => (x(10).toDouble,x(9).toDouble,x(53).toInt) }
      
      /**
       * First, check to see if our team won
       * Then increment gamesPlayed and add our homeruns to the tally
       */
      for( game <- allGames ) {
          if(game._1>game._2) teamWins = teamWins +1
          gamesPlayed = gamesPlayed +1
          teamHomeRuns = teamHomeRuns +game._3
      }
      (teamHomeRuns,teamWins/gamesPlayed)
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
    val sc = new SparkContext(new SparkConf()
      .setAppName("Blog Example: Statistics and Correlation"))
    
    // Create our input
    val mappedFile = sc.textFile(args(0)).map(_.split(","))
    
    // Transform our data
    val mappedTo = 
      mappedFile.groupBy { line => (line(3) + "," + line(0).substring(1,5)) } join
      mappedFile.groupBy { line => (line(6) + "," + line(0).substring(1,5)) } map
      calculateByTeam
    
    // Retrieve the results
    val homeruns = mappedTo map(_._1)
    val winningPercentage = mappedTo map(_._2)
    
    // Run our analysis
    val correlation = Statistics.corr(homeruns,winningPercentage,"pearson")
    println("**************Pearson coefficiant for homeruns to winning percentage " + correlation)
    
    // Print
    val homerunStats = homeruns.stats
    println("**************Mean of homeruns " + homerunStats.mean)
    println("**************Standard deviation of homeruns " + homerunStats.stdev)
    println("**************Variance of homeruns " + homerunStats.variance)

    val winningPercentageStats = winningPercentage.stats
    println("**************Mean of winning percentage " + winningPercentageStats.mean)
    println("**************Standard deviation of winning percentage " + winningPercentageStats.stdev)
    println("**************Variance of winning percentage " + winningPercentageStats.variance)
  }
}