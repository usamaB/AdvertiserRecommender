package org.usama.aidrecommender.utils

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper extends Serializable {

  val master  = "local[*]"
  val appName = "AppName"
  lazy val spark: SparkSession = {
    SparkSession.builder().master(master).appName(appName).getOrCreate()
  }
}
