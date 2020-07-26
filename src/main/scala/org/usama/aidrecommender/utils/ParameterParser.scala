package org.usama.aidrecommender.utils

import com.monovore.decline.{Command, Opts}
import cats.implicits._

case class AppParameters(
    impressionsPath: String,
    clicksPath: String,
    outputPath: String,
    topN: Int
)

/**
  * Using decline a command-line parser built on cats
  * https://github.com/bkirwi/decline
  * Creating a command and the arguments it needs
  */
object ParameterParser {

  private def getOpts() = {
    val impressionsFilePath =
      Opts
        .option[String]("impressions-file-path", "absolute path for file", "i")
    val clicksFilePath =
      Opts.option[String]("clicks-file-path", "absolute path for file", "c")
    val outputFilePath =
      Opts
        .option[String]("output-file-path", "absolute path for file", "o")
        .withDefault("recommended_advertisers.json")
    val topN =
      Opts
        .option[Int]("topN", "amount of top advertisers", "n")
        .withDefault(5)

    (impressionsFilePath, clicksFilePath, outputFilePath, topN).tupled

  }

  private def getCommand = {
    Command(name = "metrics", header = "calculate ad metrics") {
      getOpts()
    }
  }

  /**
    * Parses the applications parameters
    * In case of Failure prints error and exits the system esle return AppParameters
    */
  def parse(args: Array[String]): AppParameters = {
    getCommand.parse(args) match {
      case Left(help) =>
        System.err.println(help)
        sys.exit(1)
      case Right(parsedValue) =>
        AppParameters(
          parsedValue._1,
          parsedValue._2,
          parsedValue._3,
          parsedValue._4
        )
    }
  }

}
