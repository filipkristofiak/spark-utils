package com.filipkr.spark.utils.sql

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{coalesce, col, lit, when}
import scala.util.{Failure, Success, Try}
import org.apache.spark.storage.StorageLevel

import ColumnUtils._

trait DataFrameUtils {

  /**
   * handles problem in joining with dense cube - "join on null values"
   * @param dimCols
   * @param defaultValue
   * @param df
   * @tparam T literal type of default value
   * @return
   */
  def fillColumns[T](dimCols: Seq[String], defaultValue: T)(df: DataFrame): DataFrame =
    dimCols.foldLeft(df) { (dft, c) =>
      dft.withColumn(c, coalesce(col(c), lit(defaultValue)))
    }

  implicit class DataFrameExtended(val df: DataFrame) {

    /**
     * Apply an optional transformation to a DataFrame
     * @param fnOpt - when it's of the type Some(_.filter(booleanColumnExpression)), the transformation is applied, when it's None, the transformation is not applied
     * @return
     *
     * @author https://github.com/kotanyi (Martin Godany)
     */
    def transformOpt(fnOpt: Option[DataFrame => DataFrame]): DataFrame =
      fnOpt.map(df.transform).getOrElse(df)

    /**
     * Function returns first existing column from given alternatives, if fails, returns lit(null)
     * this can be combined with coalesce
     * @param colName column names to try
     *
     * @author https://github.com/elkozmon (Lubos Kozmon)
     */
    def tryColumns(colName: String*): Column =
      if (colName.nonEmpty) Try(df(colName.head)) match {
        case Success(v) => v
        case Failure(_) => tryColumns(colName.tail: _*)
      }
      else lit(null)

    // /**
    //  * Stores dataframe to selected partitioned table in parquet format, creates table if it does not exist
    //  * @param tableNameWithSchema table name with schema to store dataframe to
    //  * @param partitionKeys seq of keys to partition the dataframe by
    //  */
    // def storeToTable(tableNameWithSchema: String, partitionKeys: Seq[String])(implicit sparkSession: SparkSession): Unit = {
    //   /* TODO: configurable Hive/Iceberg/... */
    // }

    // /**
    //  * Stores dataframe to selected non-partitioned table in parquet format, creates table if it does not exist
    //  * @param tableNameWithSchema table name with schema to store dataframe to
    //  */
    // def storeToTable(tableNameWithSchema: String)(implicit sparkSession: SparkSession): Unit = {
    //   /* TODO: configurable Hive/Iceberg/... */
    // }

    // /**
    //  * Appends dataframe to selected partition of a partitioned table in parquet format, creates table if it does not exist
    //  * @param tableNameWithSchema table name with schema to store dataframe to
    //  * @param partitionKeys seq of keys to partition the dataframe by
    //  */
    // def appendToTable(tableNameWithSchema: String, partitionKeys: Seq[String])(implicit sparkSession: SparkSession): Unit = {
    //   /* TODO: configurable Hive/Iceberg/... */
    // }

    def fillColumn[T](colName: String, naValue: T): DataFrame =
      df.withColumn(colName, col(colName).f(naValue))

    /* replace value with 'null' on met condition */
    def nullColumn(colName: String, nullCondition: Column): DataFrame =
      df.withColumn(colName, when(!nullCondition, col(colName)))

    def roundColumn(colName: String, precision: Int = 2): DataFrame =
      df.withColumn(colName, col(colName).round(precision))

    /**
     * Explicitly pass a name that will be displayed in Spark's Storage UI.
     * @author Georg Heiler - source: https://georgheiler.com/2019/07/23/spark-descriptive-name-for-cached-dataframes/
     */
    def persistAs(name: String, storageLevel: StorageLevel = StorageLevel.DISK_ONLY): DataFrame = {
      df.sparkSession.sharedState.cacheManager
        .cacheQuery(df, Some(name), storageLevel)
      df
    }

    def DISK(): DataFrame = df.persist(org.apache.spark.storage.StorageLevel.DISK_ONLY)

    /**
     * Triggers eager evaluation of a dataframe, useful after persisting
     */
    def eval(): Unit = df.foreach(_ => ())

    // scala collection like
    def filterNot(expr: Column): DataFrame =
      df.filter(!expr)

    // more human
    def without(expr: Column): DataFrame =
      df.filter(!expr)

    def randCols(numCols: Int = 1): DataFrame = {
      val rGen  = scala.util.Random
      val rCols = df.columns.sortBy(_ => rGen.nextDouble).take(numCols)

      df.select(rCols.head, rCols.tail: _*)
    }
  }
}

object DataFrameUtils extends DataFrameUtils
