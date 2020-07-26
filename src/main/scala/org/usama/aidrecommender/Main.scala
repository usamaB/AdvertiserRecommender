package org.usama.aidrecommender

import org.apache.spark.sql.SparkSession
import org.usama.aidrecommender.io.LocalStorage
import org.usama.aidrecommender.jobs.AdvertiserRecommender
import org.usama.aidrecommender.utils.{AppParameters, ParameterParser, SparkJob}

object Main {

  /**
    * Entry Point of the application
    * create SparkSession, read and parse command-line arguments, create JobConf
    * run job(s)
    * stop SparkSession
    * @param args
    */
  def main(args: Array[String]) = {
    val params = ParameterParser.parse(args)
    val spark  = createSparkSession()
    runJobs(spark, params)
    spark.stop()
  }

  /**
    * Creates spark Session
    * TODO: Create from external Config file and move to SparkHelper
    * @return
    */
  def createSparkSession() = {
    SparkSession
      .builder()
      .appName("AdvertiserRecommender")
      .master("local[*]")
      .getOrCreate
  }

  /**
    * Runs SparkJob(s)
    */
  def runJobs(spark: SparkSession, args: AppParameters) = {
    val advertiserRecommenderJob: SparkJob = new AdvertiserRecommender
    advertiserRecommenderJob.run(
      spark,
      args,
      new LocalStorage(spark)
    )
    // can run more jobs here
  }
}
