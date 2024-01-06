package com.filipkr.spark.utils.sql.transform

import org.apache.spark.sql.{Column, ColumnName, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{array, coalesce, col, explode, lit, struct}
import org.apache.spark.sql.types.StringType

import java.time.LocalDate

import summary.Helpers._
import com.filipkr.spark.utils._

trait Transformations {

  /**
   * Extends functionality of builtin DataFrame functions describe()/summary() to be more like Pandas describe
   *
   * @param factors List of columns to be treated as factors
   */
  def getSummary(factors: String*)(df: DataFrame): DataFrame = {

    val schema = df.schema.fields

    val statsDistinct = (schema.map(_.dataType).map(typeWitness) :+ defaultStats)
      .reduce(_ ++ _)
      .keys
      .toSeq
      .sortBy(statsDistinctSort.indexOf(_))

    val statsAggs = schema.flatMap(f => dataTypeStats(f, factors))

    val statsAggDF = df.agg(statsAggs.head, statsAggs.tail: _*)

    statsAggDF.persistAs("get_summary_" + df.sparkSession.sparkContext.sparkUser + "_" + currentTimeFmt("HH:mm:ss"))
    statsAggDF.count()

    statsAggDF
      .select(
        explode(
          array(
            statsDistinct
              .map(agg =>
                struct(
                  lit(agg).as("summary") +: schema.map(field =>
                    coalesce(statsAggDF.tryColumns(field.name + "/" + agg).cast(StringType), lit("null"))
                      .as(field.name)
                  ): _*
                )
              ): _*
          )
        ).as("tmp")
      )
      .select(("summary" +: schema.map(_.name)).map(emb => col("tmp." + emb)): _*)
  } 
}

object Transformations extends Transformations
