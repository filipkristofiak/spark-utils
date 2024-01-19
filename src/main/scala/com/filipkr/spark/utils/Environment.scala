package com.filipkr.spark.utils

import org.apache.spark.sql.SparkSession

trait Environment {

  /**
   * Enable Cartesian product, i.e. join on lit(true)
   *
   * @example{{{
   *     // implicit val ss = spark  /* Exists in context */
   *     enableCrossJoin             /* Enough to call once */
   *
   *     df1.join(df2,lit(true))
   * }}}
   */
  def enableCrossJoin(implicit ss: SparkSession): Unit = ss.conf.set("spark.sql.crossJoin.enabled", "true")

  /**
   * Turns of Spark automatic DataFrame broadcasting in join
   */
  def autoBroadcastOff(implicit ss: SparkSession): Unit = ss.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

  /**
   * Allows using untyped Scala UDFs (legacy, using outType, deprecated since 3.0.0)
   * @example{{{
   *     // implicit val ss = spark  /* Exists in context */
   *     allowUntypedUDF             /* Enough to call once */
   *
   *     def removeNull: Seq[Row] => Seq[Row] = _.filterNot(_ == null)
   *     def rowSchema: DataType = ??? // schema of the row
   *
   *     df.withColumn("some_col", removeNull.udf(rowSchema)(col("some_col")))
   * }}}
   */
  def allowUntypedUDF(implicit ss: SparkSession) = ss.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
}

object Environment extends Environment
