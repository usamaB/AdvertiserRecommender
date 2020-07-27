package org.usama.airrecommender.jobs

import org.scalatest.FlatSpec
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{
  ArrayType,
  DoubleType,
  IntegerType,
  StringType
}
import org.scalatest.Matchers
import org.usama.aidrecommender.io.{LocalStorage, ReadConfig, Storage}
import org.usama.aidrecommender.jobs.AdvertiserRecommender
import org.usama.aidrecommender.utils.SparkSessionExt._

class AdvertiserRecommenderTest
    extends FlatSpec
    with Matchers
    with SharedSparkContext
    with DataFrameSuiteBase {
  override implicit def reuseContextIfPossible: Boolean = true
  val recommenderJob                                    = new AdvertiserRecommender

  "Reading Wrong Path/File" should "return Failure" in {
    val storage: Storage = new LocalStorage(spark)
    storage.read(ReadConfig("json", "invalid_path")).isFailure shouldBe (true)
  }

  "dropNaNsAndNull method" should "drop rows containing null and NaNs" in {
    val dfWithNull = spark.createDF(
      List(1, 2, null, 4, null),
      List(("numbers", IntegerType, true))
    )
    dfWithNull.transform(recommenderJob.dropNaNsAndNull()).count() shouldBe (3)
  }

  "joined DF" should "have both columns in the result" in {
    val left = spark.createDF(
      List(1, 2, null, 4, null),
      List(("numbers", IntegerType, true))
    )
    val right = spark.createDF(
      List(1, 2, null, 4, null),
      List(("ids", IntegerType, true))
    )
    val joined = left
      .transform(recommenderJob.joinOverCols(spark, right, "numbers", "ids"))
    joined.columns.contains("ids") &&
    joined.columns.contains("numbers") shouldBe (true)
  }

  "TopN Rank" should "should have count 1 as only a single value in [1,12,22,4,55] is less than 2" in {
    val n = 2
    val rankDF = spark.createDF(
      List(1, 12, 22, 4, 55),
      List(("rank", IntegerType, true))
    )
    rankDF.transform(recommenderJob.getTopNRank(n)).count() shouldBe (1)
  }

  "getRecommendedIdsList" should " return only N number of rows in a DataFrame with rank Column" in {
    val n = 2
    val data = spark.createDF(
      List(
        (1, "US", 2),
        (1, "US", 3),
        (1, "US", 4)
      ),
      List(
        ("app_id", IntegerType, true),
        ("country_code", StringType, true),
        ("advertiser_id", IntegerType, true)
      )
    )

    val expectedDF = spark.createDF(
      List(
        (1, "US", List(2, 3, 4))
      ),
      List(
        ("app_id", IntegerType, true),
        ("country_code", StringType, true),
        ("recommended_advertiser_ids", ArrayType(IntegerType), true)
      )
    )
    val resultDF = data.transform(recommenderJob.getRecommendedIdsList())
    assertDataFrameDataEquals(expectedDF, resultDF)
  }

  "calculate rank over given window" should "create a new column 'rank' with ranks and order records according to window" in {
    val window = Window.partitionBy("id").orderBy("price")
    val data = spark.createDF(
      List(
        (1, 22),
        (1, 33),
        (12, 14)
      ),
      List(
        ("id", IntegerType, true),
        ("price", IntegerType, true)
      )
    )

    val expectedDF = spark.createDF(
      List(
        (12, 14, 1),
        (1, 22, 1),
        (1, 33, 2)
      ),
      List(
        ("id", IntegerType, true),
        ("price", IntegerType, true),
        ("rank", IntegerType, true)
      )
    )
    val resultDF =
      data.transform(recommenderJob.calculateRankOverWindow(window))
    assertDataFrameEquals(expectedDF, resultDF)
  }

  "calculateAdvertiserRate" should "return a DataFrame with new column rate and impressions and revenue dropped" in {
    val n = 2
    val data = spark.createDF(
      List(
        (1, "US", 2, "asd", 2.0),
        (1, "US", 2, "xxx", 4.0),
        (1, "US", 4, "abc", 10.0)
      ),
      List(
        ("app_id", IntegerType, true),
        ("country_code", StringType, true),
        ("advertiser_id", IntegerType, true),
        ("impression_id", StringType, true),
        ("revenue", DoubleType, true)
      )
    )

    val expectedDF = spark.createDF(
      List(
        (1, "US", 2, 3.0),
        (1, "US", 4, 10.0)
      ),
      List(
        ("app_id", IntegerType, true),
        ("country_code", StringType, true),
        ("advertiser_id", IntegerType, true),
        ("rate", DoubleType, true)
      )
    )
    val resultDF = data.transform(recommenderJob.calculateAdvertiserRate(spark))
    assertDataFrameDataEquals(expectedDF, resultDF)
  }

}
