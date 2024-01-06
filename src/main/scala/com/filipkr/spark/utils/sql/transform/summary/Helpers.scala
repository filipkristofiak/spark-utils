package com.filipkr.spark.utils.sql.transform.summary

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Helpers {

  lazy val statsDistinctSort = Seq(
    "count",
    "unique",
    "min",
    "max",
    "avg",
    "stddev",
    "nulls",
    "factors",
    "min_size",
    "max_size",
    "avg_size",
    "length_0",
    "factors",
    "type"
  )

  lazy val defaultStats = Seq("count", "nulls", "type").map(_ -> true).toMap

  def dataTypeStats(field: StructField, factors: Seq[String]): Seq[Column] = field.dataType match {

    case IntegerType | LongType =>
      Seq(
        count(col(field.name)).as(field.name + "/count"),
        countDistinct(col(field.name)).as(field.name + "/unique"),
        min(col(field.name)).as(field.name + "/min"),
        round(mean(col(field.name)), 2).as(field.name + "/avg"),
        max(col(field.name)).as(field.name + "/max"),
        round(stddev(col(field.name)), 2).as(field.name + "/stddev"),
        sum(when(col(field.name).isNull, lit(1)).otherwise(lit(0))).as(field.name + "/nulls"),
        lit(field.dataType.simpleString).as(field.name + "/type")
      )

    case DoubleType =>
      Seq(
        count(col(field.name)).as(field.name + "/count"),
        min(col(field.name)).as(field.name + "/min"),
        mean(col(field.name)).as(field.name + "/avg"),
        max(col(field.name)).as(field.name + "/max"),
        stddev(col(field.name)).as(field.name + "/stddev"),
        sum(when(col(field.name).isNull, lit(1)).otherwise(lit(0))).as(field.name + "/nulls"),
        lit(field.dataType.simpleString).as(field.name + "/type")
      )

    case TimestampType =>
      Seq(
        count(col(field.name)).as(field.name + "/count"),
        min(col(field.name)).as(field.name + "/min"),
        max(col(field.name)).as(field.name + "/max"),
        sum(when(col(field.name).isNull, lit(1)).otherwise(lit(0))).as(field.name + "/nulls"),
        lit(field.dataType.simpleString).as(field.name + "/type")
      )

    case StringType =>
      Seq(
        count(col(field.name)).as(field.name + "/count"),
        countDistinct(col(field.name)).as(field.name + "/unique"),
        sum(when(col(field.name).isNull, lit(1)).otherwise(lit(0))).as(field.name + "/nulls"),
        lit(field.dataType.simpleString).as(field.name + "/type")
      ) ++ (if (factors.contains(field.name)) Seq(collect_set(col(field.name)).as(field.name + "/factors"))
            else Seq.empty[Column])

    case DateType =>
      Seq(
        count(col(field.name)).as(field.name + "/count"),
        countDistinct(col(field.name)).as(field.name + "/unique"),
        min(col(field.name)).as(field.name + "/min"),
        max(col(field.name)).as(field.name + "/max"),
        sum(when(col(field.name).isNull, lit(1)).otherwise(lit(0))).as(field.name + "/nulls"),
        lit(field.dataType.simpleString).as(field.name + "/type")
      ) ++ (if (factors.contains(field.name)) Seq(collect_set(col(field.name)).as(field.name + "/factors"))
            else Seq.empty[Column])

    case ArrayType(_, _) =>
      Seq(
        count(col(field.name)).as(field.name + "/count"),
        min(size(col(field.name))).as(field.name + "/min_size"),
        round(mean(size(col(field.name))), 2).as(field.name + "/avg_size"),
        max(size(col(field.name))).as(field.name + "/max_size"),
        sum(when(col(field.name).isNull, lit(1)).otherwise(lit(0))).as(field.name + "/nulls"),
        sum(when(size(col(field.name)) === 0, lit(1)).otherwise(lit(0))).as(field.name + "/length_0"),
        lit(field.dataType.simpleString).as(field.name + "/type")
      )

    case _ =>
      Seq(
        count(col(field.name)).as(field.name + "/count"),
        sum(when(col(field.name).isNull, lit(1)).otherwise(lit(0))).as(field.name + "/nulls"),
        lit(field.dataType.simpleString).as(field.name + "/type")
      )
  }

  def typeWitness(dt: DataType): Map[String, Boolean] =
    (dt match {
      case ArrayType(_, _) => Seq("min_size", "avg_size", "max_size", "length_0")
      case StringType => Seq("unique", "factors")
      case DateType => Seq("unique", "min", "max", "factors")
      case TimestampType => Seq("min", "max")
      case IntegerType | LongType => Seq("unique", "min", "avg", "max", "stddev")
      case DoubleType => Seq("min", "avg", "max", "stddev")
      case _ => Seq.empty[String]
    }).map(_ -> true).toMap
}
