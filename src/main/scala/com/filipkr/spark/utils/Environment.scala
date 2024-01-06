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
  //TODO: move to appropriate package
  def enableCrossJoin(implicit ss: SparkSession): Unit = ss.conf.set("spark.sql.crossJoin.enabled", "true")

  /**
   * Turns of Spark automatic DataFrame broadcasting in join
   */
  def autoBroadcastOff(implicit ss: SparkSession): Unit = ss.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
}

object Environment extends Environment
