package com.filipkr.spark.utils.time

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.{DayOfWeek, Duration, Instant, LocalDate, OffsetDateTime, ZoneId}
import java.time.format.DateTimeFormatter

import scala.util.Try

trait TimeUtils {

  final val ymdFormatPattern: String = "yyyy-MM-dd"
  final val ymFormatPattern: String  = "yyyy-MM"
  final val yFormatPattern: String   = "yyyy"

  final val yqmdFormatPattern: String = "yyyy-'Q'Q-MM-dd"
  final val yqmFormatPattern: String  = "yyyy-'Q'Q-MM"
  final val yqFormatPattern: String   = "yyyy-'Q'Q"

  /**
   * java.time 'yyyy-MM-dd' format pattern to use when working with java.time.LocalDate
   *
   * @example {{{ LocalDate.now.format(ymdFormatter) }}}
   */
  final val ymdFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(ymdFormatPattern)

  /**
   * java.time 'yyyy-MM' format pattern to use when working with java.time.LocalDate
   *
   * @example {{{ LocalDate.now.format(ymFormatter) }}}
   */
  final val ymFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(ymFormatPattern)

  /**
   * java.time 'yyyy' format pattern to use when working with java.time.LocalDate
   *
   * @example {{{ LocalDate.now.format(yFormatter) }}}
   */
  final val yFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(yFormatPattern)

  /**
   * java.time "yyyy-'Q'Q-MM-dd" format pattern to use when working with java.time.LocalDate
   *
   * @example {{{ LocalDate.now.format(ymdFormatter) }}}
   */
  final val yqmdFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(yqmdFormatPattern)

  /**
   * java.time "yyyy-'Q'Q-MM" format pattern to use when working with java.time.LocalDate
   *
   * @example {{{ LocalDate.now.format(ymFormatter) }}}
   */
  final val yqmFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(yqmFormatPattern)

  /**
   * java.time "yyyy-'Q'Q" format pattern to use when working with java.time.LocalDate
   *
   * @example {{{ LocalDate.now.format(yFormatter) }}}
   */
  final val yqFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(yqFormatPattern)

  /**
   *  java.time 'yyyy-MM-dd'T'HH:mm:ssxx' format pattern to use when working with java.time.ZonedDateTime
   *
   *  @example {{{ timestampFormatter.format(ZonedDateTime.now) }}}
   */
  final val timestampFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssxx")

  /**
   * java.text 'hh:mm:ss.S' formatter for java.sql.Timestamp
   *
   * @example {{{ daytimeFormatter.format(new java.sql.Timestamp(System.currentTimeMillis())) }}}
   */
  final val daytimeFormatter = new SimpleDateFormat("HH:mm:ss.S")

  /**
   * Returns dates between two dates, good for iterations
   * over multiple dates (python pandas style)
   */
  def dateRange(dateSince: LocalDate, dateUntil: LocalDate): Seq[LocalDate] =
    Stream
      .iterate(dateSince)(_.plusDays(1))
      .takeWhile(!_.isAfter(dateUntil))

  val getDaysBetween: (Timestamp, Timestamp) => Option[Long] = (start, end) => Try(Duration.between(start.toInstant, end.toInstant).toDays).toOption

  /**
   * @return Returns whole hours passed between two timestamps
   * @example getHourseBetween.udf($"timestamp1", $"timestamp2")
   */
  val getHoursBetween: (Timestamp, Timestamp) => Option[Long] = (start, end) => Try(Duration.between(start.toInstant, end.toInstant).toHours).toOption

  /**
   * Parses DataFrame friendly timestamp from strings
   * @author https://github.com/zlosim (Michal Vince)
   */
  val makeTimestamp: String => Timestamp = { x =>
    Try(new Timestamp(OffsetDateTime.parse(x).toInstant.toEpochMilli))
      .getOrElse(null)
  }

  /**
   * format Spark Date column to string using DateTime Format Pattern,
   * i.e. ymdFormatPattern = "yyyy-MM-dd", ymFormatPattern = "yyyy-MM", "yyyy/MM"
   *
   * @example {{{
   *           spark
   *             .table("da.us_activity_diff")
   *             .select(
   *                $"date",
   *                formatDate(ymFormatPattern).udf($"date").as("partition_date")
   *             )
   * }}}
   */
  val formatDate: String => java.sql.Date => String = {
    case "yyyy-MM"         => _.toLocalDate.format(TimeUtils.ymFormatter)
    case "yyyy-'Q'Q"       => _.toLocalDate.format(TimeUtils.yqFormatter)
    case "yyyy-'Q'Q-MM"    => _.toLocalDate.format(TimeUtils.yqmFormatter)
    case "yyyy-'Q'Q-MM-dd" => _.toLocalDate.format(TimeUtils.yqmdFormatter)
    case "yyyy-MM-dd"      => _.toLocalDate.format(TimeUtils.ymdFormatter)
    case "yyyy"            => _.toLocalDate.format(TimeUtils.yFormatter)
    case fmtPattern        => _.toLocalDate.format(DateTimeFormatter.ofPattern(fmtPattern))
  }

  def currentTimeFmt(): String = daytimeFormatter.format(new Timestamp(System.currentTimeMillis()))

  def currentTimeFmt(pattern: String): String =
    (new SimpleDateFormat(pattern))
      .format(new Timestamp(System.currentTimeMillis()))

  /**
   * @param block
   * @return
   * @author https://github.com/elkozmon (Lubos Kozmon)
   */
  def printExecutionTime[R](block: => R): R = {
    val t0     = Instant.now()
    val result = block
    val t1     = Instant.now()

    val duration = Duration.between(t0, t1)

    val sb      = new StringBuilder()
    val hours   = duration.toHours    % 24
    val minutes = duration.toMinutes  % 60
    val seconds = duration.getSeconds % 60
    val millis  = duration.toMillis   % 1000

    if (hours > 0) sb.append(s"${hours}h ")
    if (minutes > 0) sb.append(s"${minutes}m ")
    if (seconds > 0) sb.append(s"${seconds}s ")
    sb.append(s"${millis}ms")

    println("Execution started at " + daytimeFormatter.format(new Timestamp(t0.toEpochMilli)))
    println("Execution finished at " + daytimeFormatter.format(new Timestamp(t1.toEpochMilli)))
    println("Elapsed time: " + sb.toString)
    result
  }
}

object TimeUtils extends TimeUtils
