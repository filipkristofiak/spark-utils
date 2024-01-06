package com.filipkr.spark.utils.sql

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{call_udf, coalesce, col, concat, lit, log, pmod, pow, sum, to_date, unix_timestamp, when}
import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.sql.types.{StringType, TimestampType}
import java.time.LocalDate

import com.filipkr.spark.utils.time.TimeUtils.{ymdFormatter, ymFormatter}

trait ColumnUtils {

  def END_OF_DAY(dateCol: Column): Column = concat(dateCol.cast(StringType), lit(" 23:59:59")).cast(TimestampType)

  def END_OF_DAY(date: LocalDate): Column =
    concat(lit(date.format(ymdFormatter)), lit(" 23:59:59")).cast(TimestampType)

  def START_OF_DAY(dateCol: Column): Column =
    when(to_date(col("register_time")) === dateCol.cast("Date"), col("register_time"))
      .otherwise(concat(dateCol.cast("Date").cast("String"), lit(" 00:00:00")).cast("Timestamp"))

  def START_OF_DAY(date: LocalDate): Column =
    when(to_date(col("register_time")) === date.format(ymdFormatter), col("register_time"))
      .otherwise(lit(date.format(ymdFormatter) + " 00:00:00"))
      .cast("Timestamp")

  def median(baseCol: Column): Column = call_udf("percentile", baseCol, lit(0.5))

  /**
   * Aggregation function to calculate percentile over baseCol column using spark builtin 'percentile' udf
   *
   * @param baseCol
   * @param percentile as column expression from interval [0,1]
   * @example{{{
   *    val myDF: DataFrame = /* player_id, is_payer, gems_spent */
   *
   *    myDF
   *      .groupBy("is_payer")
   *      .agg(
   *        count(col("player_id")).as("count"),
   *        percentile(col("gems_spent"),lit(0.75)).as("gems_spent_3rd_quartile")
   *      )
   * }}}
   */
  def percentile(baseCol: Column, percentile: Column): Column = call_udf("percentile", baseCol, percentile)

  /**
   * Aggregation function to calculate percentile over baseCol column using spark builtin 'percentile' udf
   *
   * @param baseCol
   * @param percentile as double value from interval [0,1]
   * @example{{{
   *    val myDF: DataFrame = /* player_id, is_payer, gems_spent */
   *
   *    myDF
   *      .groupBy("is_payer")
   *      .agg(
   *        count(col("player_id")).as("count"),
   *        percentile(col("gems_spent"), 0.25).as("gems_spent_1st_quartile")
   *      )
   * }}}
   */
  def percentile(baseCol: Column, percentile: Double): Column = call_udf("percentile", baseCol, lit(percentile))

  /**
   * Aggregation function to calculate percentile over baseCol column using spark builtin 'percentile_approx' udf
   *
   * @param baseCol
   * @param percentile as column expression from interval [0,1]
   * @example{{{
   *    val myDF: DataFrame = /* player_id, is_payer, gems_spent */
   *
   *    myDF
   *      .groupBy("is_payer")
   *      .agg(
   *        count(col("player_id")).as("count"),
   *        percentile_approx(col("gems_spent"),lit(0.75)).as("gems_spent_3rd_quartile")
   *      )
   * }}}
   */
  def percentile_approx(baseCol: Column, percentile: Column): Column =
    call_udf("percentile_approx", baseCol, percentile)

  /**
   * Aggregation function to calculate percentile over baseCol column using spark builtin 'percentile_approx' udf
   *
   * @param baseCol
   * @param percentile as double value from interval [0,1]
   * @example{{{
   *    val myDF: DataFrame = /* player_id, is_payer, gems_spent */
   *
   *    myDF
   *      .groupBy("is_payer")
   *      .agg(
   *        count(col("player_id")).as("count"),
   *        percentile_approx(col("gems_spent"), 0.25).as("gems_spent_1st_quartile")
   *      )
   * }}}
   */
  def percentile_approx(baseCol: Column, percentile: Double): Column =
    call_udf("percentile_approx", baseCol, lit(percentile))

  /**
   * Numeric column rounded down to multiples of given step
   * @param baseCol
   * @param step
   */
  def STEP_CATEGORY(baseCol: Column, step: Int): Column = baseCol - pmod(baseCol, lit(step))

  /**
   * Numeric column rounded down to exponential steps of given base
   * @param baseCol
   * @param base
   * @param castTo
   */
  def LOG_CATEGORY(baseCol: Column, base: Double, castTo: String = "Long"): Column =
    coalesce(pow(lit(base), log(base, baseCol).cast("Long")).cast(castTo), baseCol)

  /**
   * Conditional column projection expression. Meant primarily for manual pivot aggregation
   *
   * @param columnToInclude Column expression to be included/excluded
   * @param evidenceIndicator Boolean column expression indicating inclusion
   * @example {{{
   *         myDF.groupBy(col("date"))
   *           .agg(
   *             countDistinct(include(col("user_id), col("payer"))).as("payers"),
   *             countDistinct(include(col("user_id), !col("payer"))).as("non-payers"),
   *             countDistinct(include(col("user_id), col("payer").isNull)).as("n/a"),
   *             countDistinct(col("user_id")).as("all")
   *           )
   * }}}
   */
  def include(columnToInclude: Column, evidenceIndicator: Column): Column =
    when(evidenceIndicator, columnToInclude).otherwise(null)

  implicit class ColumnExt(c: Column) {
    def rDiv(c2: Column, precision: Int): Column        = org.apache.spark.sql.functions.round(c.cast("Double") / c2.cast("Double"), precision)
    def round(precision: Int): Column                   = org.apache.spark.sql.functions.round(c, precision)
    def f[T](value: T): Column                          = org.apache.spark.sql.functions.coalesce(c, lit(value))
    def coalesce(c2: Column): Column                    = org.apache.spark.sql.functions.coalesce(c, c2)
    def perc(total: Column, precision: Int = 2): Column = (lit(100.0) * c.cast("Double")).rDiv(total, precision)

    /**
     * grand total / window total percentage
     * @param precision
     * @param gtWindow
     */
    def gtPerc(precision: Int = 2, gtWindow: WindowSpec = Expressions.everything): Column = c.perc(sum(c).over(gtWindow), precision)

    def LOG_CATEGORY(base: Double, castTo: String = "Long"): Column = ColumnUtils.LOG_CATEGORY(c, base, castTo)

    def STEP_CATEGORY(base: Int): Column = ColumnUtils.STEP_CATEGORY(c, base)

    def VALUE_WHEN[T](value: T, condFnc: Column => Column): Column = when(condFnc(c), lit(value)).otherwise(c)

    def LIMITER(sup: Long): Column            = when(c > sup, lit(sup)).otherwise(c)
    def LIMITER(inf: Long, sup: Long): Column = when(c < inf, lit(inf)).when(c > sup, lit(sup)).otherwise(c)

    def SUBSTITUTE[T](value: T, substitution: T): Column = when(c === value, lit(substitution)).otherwise(c)

    def INTERVAL_CATEGORY(intervalLowerBounds: Seq[Double], fallThroughValue: Double = 0.0): Column = {
      (intervalLowerBounds.reverse.tail
        .foldLeft(when(c >= intervalLowerBounds.last, lit(intervalLowerBounds.last))) { (cExp, lb) =>
          cExp.when(c >= lb, lit(lb))
        })
        .otherwise(lit(fallThroughValue))
    }

    def hoursFrom(fromCol: Column): Column = ((unix_timestamp(c) - unix_timestamp(fromCol)) / lit(3600)).cast("Long")
  }

}

object ColumnUtils extends ColumnUtils
