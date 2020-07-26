package org.usama.aidrecommender.utils

import org.apache.spark.sql.SparkSession
import org.usama.aidrecommender.io.Storage

trait SparkJob {
  def appName: String
  def run(
      spark: SparkSession,
      args: AppParameters,
      storage: Storage
  )
}
