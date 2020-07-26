package org.usama.aidrecommender.io

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.util.Try

case class ReadConfig(
    format: String,
    path: String,
    options: Map[String, String] = Map()
)

trait Reader {
  def read(config: ReadConfig): Try[DataFrame]
}

case class WriteConfig(
    format: String,
    path: String,
    mode: SaveMode = SaveMode.Overwrite,
    options: Map[String, String] = Map()
)

trait Writer {
  def write(df: DataFrame, config: WriteConfig): Unit
}

case class Config(read: ReadConfig, write: WriteConfig)

trait Storage extends Reader with Writer {
  val spark: SparkSession
}
