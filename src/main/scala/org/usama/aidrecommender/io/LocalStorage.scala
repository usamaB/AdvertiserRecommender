package org.usama.aidrecommender.io

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

/**
  * @param spark spark session or TestSparkSession for unit/integration tests
  * Does I/O from LocalFileSystem
  */
class LocalStorage(override val spark: SparkSession) extends Storage {
  override def read(config: ReadConfig): Try[DataFrame] = {
    Try {
      spark.read
        .format(config.format)
        .options(config.options)
        .load(config.path)
    }
  }

  override def write(df: DataFrame, config: WriteConfig): Unit = {
    df.write
      .format(config.format)
      .mode(config.mode)
      .options(config.options)
      .save(config.path)
  }
}
