package com.filipkr.spark.utils.sql

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.expressions.Window

trait Expressions {

  val everything = Window.partitionBy(lit("anything"))
}

object Expressions extends Expressions
