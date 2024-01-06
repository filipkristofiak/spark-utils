package com.filipkr.spark.utils.implicits

import org.apache.spark.sql.{Column, ColumnName, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{coalesce, lit}
import java.time.LocalDate
import com.filipkr.spark.utils.time.TimeUtils.{ymdFormatter, ymFormatter}

trait CoreUtils {

  implicit class StringToEventDataColumn(val sc: StringContext) {
    def e(eventDataFieldName: String*): ColumnName =
      new ColumnName("event_data." + sc.s(eventDataFieldName: _*))

    def fill_0(colName: String*): Column =
      coalesce(new ColumnName(sc.s(colName: _*)), lit(0))
  }

  implicit class SeqExtended[T](iter: Seq[T]) {
    def printEach: Unit = iter.foreach(println)
  }

  implicit class ArrayExtended[T](iter: Array[T]) {
    def printEach: Unit = iter.foreach(println)
  }

  implicit def toLD(s: String): LocalDate = LocalDate.parse(s)

  implicit class LocalDateExtended(date: LocalDate) {
    def ymd(): String          = date.format(ymdFormatter)
    def ym(): String           = date.format(ymFormatter)
    def nextMonth(): LocalDate = date.withDayOfMonth(1).plusMonths(1)
  }

}

object CoreUtils extends CoreUtils
