package org.usama.aidrecommender.jobs

import java.io.{BufferedWriter, File, FileWriter}

import io.circe.parser._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.usama.aidrecommender.io.{ReadConfig, Storage, WriteConfig}
import org.usama.aidrecommender.utils.{AppParameters, MyFileUtil, SparkJob}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._

import scala.io.Source
import scala.util.{Failure, Success}

class AdvertiserRecommender extends SparkJob {
  override def appName: String = "AdvertiserRecommender"
  Logger.getRootLogger.setLevel(Level.WARN)

  /**
    * Main function of a Job
    * Separate IO from Transformations(Separations of Concerns)
    * Storage can be any e.g Local/S3/HDFS
    */
  override def run(
      spark: SparkSession,
      args: AppParameters,
      storage: Storage
  ) = {

    val clicksDF =
      storage
        .read(
          ReadConfig(
            "json",
            args.clicksPath,
            Map("multiLine" -> "true")
          )
        ) match {
        case Success(df) => df
        case Failure(ex) =>
          println(s"FileNotFound ${ex.getLocalizedMessage}")
          sys.exit(1)
      }
    val impressionsDF =
      storage
        .read(
          ReadConfig(
            "json",
            args.impressionsPath,
            Map("multiLine" -> "true")
          )
        ) match {
        case Success(df) => df
        case Failure(ex) =>
          println(s"FileNotFound ${ex.getLocalizedMessage}")
          sys.exit(1)
      }

    val cleanedDFs = preProcessing(clicksDF, impressionsDF)
    val resultDF =
      transformations(spark, cleanedDFs._1, cleanedDFs._2, args.topN)

    storage.write(
      resultDF,
      WriteConfig("json", "outputDir")
    )

    convertToSingleFile(spark, "outputDir", args.outputPath + ".singleline")
//    //Spark Code is finished here.
//    //Next is just converting single line json to multi line json and writing a file

    convertToMultiLineJson(args.outputPath)

  }

  /**
    * PreProcessing Stage
    * Cleaning(drop null) data.
    * Can do something more with dirty data
    */
  private def preProcessing(clicksDF: DataFrame, impressionsDF: DataFrame) = {
    (
      clicksDF.transform(dropNaNsAndNull()),
      impressionsDF.transform(dropNaNsAndNull()).filter("country_code != ''")
      //drop rows with null and cou
    )
  }

  /**
    * All Transformations go here
    * Separate Transformations so they can be tested
    * Return result
    */
  private def transformations(
      spark: SparkSession,
      clicksDF: DataFrame,
      impressionsDF: DataFrame,
      topN: Int
  ): DataFrame = {
    import spark.implicits._

    val joinedDF =
      impressionsDF.transform(
        joinOverCols(spark, clicksDF, "id", "impression_id")
      )

    val groupedDF = joinedDF
      .transform(calculateAdvertiserRate(spark))

    val window =
      Window.partitionBy($"app_id", $"country_code").orderBy($"rate".desc)

    val windowResult = groupedDF
      .transform(calculateRankOverWindow(window))
      .transform(getTopNRank(topN))

    windowResult.transform(getRecommendedIdsList())
  }

  /** Transformations Start here each is testable */

  def dropNaNsAndNull()(df: DataFrame) = {
    df.na.drop()
  }

  def joinOverCols(
      spark: SparkSession,
      right: DataFrame,
      colLeft: String,
      colRight: String
  )(
      left: DataFrame
  ) = {
    import spark.implicits._

    left.join(right, $"${colLeft}" === $"${colRight}")
  }

  def calculateAdvertiserRate(
      spark: SparkSession
  )(df: DataFrame) = {
    import spark.implicits._

    df.groupBy($"app_id", $"country_code", $"advertiser_id")
      .agg(
        count($"impression_id").as("impression_count"),
        sum($"revenue").as("total_revenue")
      )
      .withColumn("rate", $"total_revenue" / $"impression_count")
      .drop("impression_count", "total_revenue")
  }

  def calculateRankOverWindow(window: WindowSpec)(df: DataFrame) = {
    df.withColumn("rank", dense_rank.over(window))
  }
  def getTopNRank(n: Int)(df: DataFrame) = {
    df.where(s"rank <= ${n}")
  }
  def getRecommendedIdsList()(df: DataFrame): DataFrame = {
    df.groupBy("app_id", "country_code")
      .agg(collect_list("advertiser_id").as("recommended_advertiser_ids"))
  }

  /**
    * Hadoop FS Utility
    * converts Spark Writer Output Dir to a SingleFile on the Provided FileSystem(Local In this case)
    * @param header is used for CSV Headers
    */
  private def convertToSingleFile(
      spark: SparkSession,
      srcPath: String,
      dstPath: String,
      header: String = ""
  ) = {
    new MyFileUtil().merge(spark, srcPath, dstPath, header)
  }

  /**
    * Reads single line json file
    * converts it to multiline json
    * and writes to a file
    * @param path
    */
  private def convertToMultiLineJson(path: String) = {
    val source = Source.fromFile(path + ".singleline")
    val writer = new BufferedWriter(
      new FileWriter(new File(path))
    )
    val jsonList = source.getLines.toList
    val numLines = jsonList.size

    writer.append('[')
    writer.newLine()
    for ((json, index) <- jsonList.zipWithIndex) {
      val parsedJson = parse(json) match {
        case Left(failure) =>
          System.err.println(failure.getLocalizedMessage)
          sys.exit(1)
        case Right(json) => json.spaces2
      }
      writer.write(parsedJson)
      if (index < numLines - 1)
        writer.append(',')
      writer.newLine()
    }
    writer.append(']')

    source.close()
    writer.close()
  }

}
