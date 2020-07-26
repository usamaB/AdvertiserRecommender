package org.usama.aidrecommender.utils

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DataType, StructField, StructType}

object SparkSessionExt {

  implicit class SparkSessionMethods(spark: SparkSession) {

    private def asRows[U](values: List[U]): List[Row] = {
      values.map {
        case x: Row     => x.asInstanceOf[Row]
        case y: Product => Row(y.productIterator.toList: _*)
        case a          => Row(a)
      }
    }

    private def asSchema[U <: Product](fields: List[U]): List[StructField] = {
      fields.map {
        case x: StructField => x.asInstanceOf[StructField]
        case (name: String, dataType: DataType, nullable: Boolean) =>
          StructField(
            name,
            dataType,
            nullable
          )
      }
    }

    /**
      * Creates a DataFrame, similar to createDataFrame, but with better syntax
      * The `createDF` method can also be used with lists of `Row` and `StructField` objects.
      */
    def createDF[U, T <: Product](
        rowData: List[U],
        fields: List[T]
    ): DataFrame = {
      spark.createDataFrame(
        spark.sparkContext.parallelize(asRows(rowData)),
        StructType(asSchema(fields))
      )
    }

    /**
      * Creates an empty DataFrame given schema fields
      * This is a handy fallback when you fail to read from a data source
      */
    def createEmptyDF[T <: Product](fields: List[T]): DataFrame = {
      spark.createDataFrame(
        spark.sparkContext.emptyRDD[Row],
        StructType(asSchema(fields))
      )
    }

  }

}
