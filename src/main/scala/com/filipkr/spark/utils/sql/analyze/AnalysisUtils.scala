package com.filipkr.spark.utils.sql.analyze

import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, collect_list, lit, struct}

import com.filipkr.spark.utils.sql.analyze.{ExponentialMovingAverage => EMA}
import com.filipkr.spark.utils._


trait AnalysisUtils {

  /**
   * Dataframe transformation that adds exponential moving average columns for given metrics over 'date' column.
   */
  def exponentialMovingAverage(metrics: String*)(dimensions: String*)(df: DataFrame): DataFrame = 
    df.transform(EMA.exponentialMovingAverage(metrics:_*)(dimensions:_*))
}

object AnalysisUtils extends AnalysisUtils
