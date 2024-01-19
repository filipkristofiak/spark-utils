package com.filipkr.spark.utils.sql.analyze

import java.time.temporal.ChronoUnit
import java.time.LocalDate
import java.sql.Date

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.functions.{col, collect_list, lit, struct, udf}

import com.filipkr.spark.utils.time.TimeUtils.getDaysBetween
import com.filipkr.spark.utils.UdfUtils._

object ExponentialMovingAverage {

  val ema: (ChronoUnit, Int, Int, Double, Seq[String]) => (Date, Seq[Row]) => Seq[Double] =
    (chronoUnit, periodSize, periodCount, alpha, metricNames) =>
      (date, columnPartitionValues) => {
        def daysBetween(start: LocalDate, end: LocalDate, chronoUnit: ChronoUnit): Long = chronoUnit.between(start, end)

        val sliceSize = columnPartitionValues.size

        val adjustedWeights = (0 until sliceSize)
          .map(idx =>
            (
              idx,
              daysBetween(
                columnPartitionValues(idx.toInt).getAs[Date]("date").toLocalDate,
                date.toLocalDate,
                chronoUnit
              ) / periodSize.toInt // TODO: fix for date-sparse data
            )
          )
          .foldLeft(new Array[Double](sliceSize)) { (accumulator, exp) =>
            accumulator(exp._1) = Math.pow(1 - alpha, exp._2);
            accumulator
          }

        // adjusted weights can have missing elements (sparse)
        val weightSum = (0 until periodCount).map(scala.math.pow((1 - alpha), _)).foldLeft(0.0)(_ + _) * periodSize

        (adjustedWeights, columnPartitionValues.map(x => metricNames.map(_metric => x.getAs[Double](_metric)))).zipped.toSeq
          .map(x => x._2.map(_ * x._1))
          .foldLeft(new Array[Double](metricNames.size)) { (accumulator, summands) =>
            (0 until accumulator.size).foreach(i => accumulator(i) += summands(i))
            accumulator
          }
          .map(_ / weightSum)
      }

  def expandColumns(baseColName: String, columnNames: Seq[String])(df: DataFrame): DataFrame =
    (0 until columnNames.size).foldLeft(df) { (_df, index) =>
      _df.withColumn(columnNames(index), col(baseColName).getItem(index))
    }

  /**
   * Dataframe transformation that adds exponential moving average columns for given metrics over 'date' column.
   */
  def exponentialMovingAverage(metrics: String*)(dimensions: String*)(dataFrame: DataFrame): DataFrame = {
    import java.time.temporal.ChronoUnit.DAYS

    val alpha       = 0.3 // ewm factor
    val periodCount = 12
    val periodSize  = 28
    val windowSize  = periodCount * periodSize

    val sliceWindow = Window
      .partitionBy(dimensions.head, dimensions.tail: _*)
      .orderBy(getDaysBetween.udf(lit("2010-01-01").cast("date"), col("date")))
      .rangeBetween(1 - windowSize, 0)

    val exponentialMovingAverageSuffix = "_ema"

    def SLICE(metrics: Seq[String]): Column =
      collect_list(struct(col("date").as("date") +: metrics.map(c => col(c).as(c)): _*).as("date_metrics"))
        .over(sliceWindow)

    dataFrame
      .withColumn(
        "ema_dump",
        ema(DAYS, periodSize, periodCount, alpha, metrics).udf(col("date"), SLICE(metrics))
      )
      .transform(expandColumns("ema_dump", metrics.map(_ + exponentialMovingAverageSuffix)))
      .drop("ema_dump")
  }
}
