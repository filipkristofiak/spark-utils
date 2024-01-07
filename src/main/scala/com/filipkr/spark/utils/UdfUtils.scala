package com.filipkr.spark.utils

import org.apache.spark.sql.functions
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.DataType

import scala.reflect.runtime.universe.TypeTag

/**
 * @author https://github.com/elkozmon (Lubos Kozmon) - original idea 
 *         of transforming pure scala funtions to user defined Column
 *         functions in single statement
 * @example {{{ myPureFunction.udf($"some_column") }}}
 *
 * @author filipkr - add udf conversion with explicit out schema for
 *         functions with arguments of unsafe type, i.e. o.a.s.s.Row
 */
trait UdfUtils {

  /**
   * @param fn function to be transformed to UserDefinedFunction
   */
  implicit class UdfFunction0[+R: TypeTag](fn: Function0[R]) {
    def udf: UserDefinedFunction = functions.udf(fn)

    /**
     * @param outType forced output schema
     * @return
     */
    def udf(outType: DataType): UserDefinedFunction = functions.udf(fn, outType)
  }

  implicit class UdfFunction1[-T1: TypeTag, +R: TypeTag](fn: Function1[T1, R]) {
    def udf: UserDefinedFunction = functions.udf(fn)

    /**
     * @param outType forced output schema
     * @return
     */
    def udf(outType: DataType): UserDefinedFunction = functions.udf(fn, outType)
  }

  implicit class UdfFunction2[-T1: TypeTag, -T2: TypeTag, +R: TypeTag](fn: Function2[T1, T2, R]) {
    def udf: UserDefinedFunction = functions.udf(fn)

    /**
     * @param outType forced output schema
     * @return
     */
    def udf(outType: DataType): UserDefinedFunction = functions.udf(fn, outType)
  }

  implicit class UdfFunction3[-T1: TypeTag, -T2: TypeTag, -T3: TypeTag, +R: TypeTag](fn: Function3[T1, T2, T3, R]) {
    def udf: UserDefinedFunction = functions.udf(fn)

    /**
     * @param outType forced output schema
     * @return
     */
    def udf(outType: DataType): UserDefinedFunction = functions.udf(fn, outType)
  }

  implicit class UdfFunction4[-T1: TypeTag, -T2: TypeTag, -T3: TypeTag, -T4: TypeTag, +R: TypeTag](fn: Function4[T1, T2, T3, T4, R]) {
    def udf: UserDefinedFunction = functions.udf(fn)

    /**
     * @param outType forced output schema
     * @return
     */
    def udf(outType: DataType): UserDefinedFunction = functions.udf(fn, outType)
  }

  implicit class UdfFunction5[-T1: TypeTag, -T2: TypeTag, -T3: TypeTag, -T4: TypeTag, -T5: TypeTag, +R: TypeTag](
      fn: Function5[T1, T2, T3, T4, T5, R]
  ) {
    def udf: UserDefinedFunction = functions.udf(fn)

    /**
     * @param outType forced output schema
     * @return
     */
    def udf(outType: DataType): UserDefinedFunction = functions.udf(fn, outType)
  }

  implicit class UdfFunction6[
      -T1: TypeTag,
      -T2: TypeTag,
      -T3: TypeTag,
      -T4: TypeTag,
      -T5: TypeTag,
      -T6: TypeTag,
      +R: TypeTag
  ](fn: Function6[T1, T2, T3, T4, T5, T6, R]) {
    def udf: UserDefinedFunction = functions.udf(fn)

    /**
     * @param outType forced output schema
     * @return
     */
    def udf(outType: DataType): UserDefinedFunction = functions.udf(fn, outType)
  }

  implicit class UdfFunction7[
      -T1: TypeTag,
      -T2: TypeTag,
      -T3: TypeTag,
      -T4: TypeTag,
      -T5: TypeTag,
      -T6: TypeTag,
      -T7: TypeTag,
      +R: TypeTag
  ](fn: Function7[T1, T2, T3, T4, T5, T6, T7, R]) {
    def udf: UserDefinedFunction = functions.udf(fn)

    /**
     * @param outType forced output schema
     * @return
     */
    def udf(outType: DataType): UserDefinedFunction = functions.udf(fn, outType)
  }

  implicit class UdfFunction8[
      -T1: TypeTag,
      -T2: TypeTag,
      -T3: TypeTag,
      -T4: TypeTag,
      -T5: TypeTag,
      -T6: TypeTag,
      -T7: TypeTag,
      -T8: TypeTag,
      +R: TypeTag
  ](fn: Function8[T1, T2, T3, T4, T5, T6, T7, T8, R]) {
    def udf: UserDefinedFunction = functions.udf(fn)

    /**
     * @param outType forced output schema
     * @return
     */
    def udf(outType: DataType): UserDefinedFunction = functions.udf(fn, outType)
  }

  implicit class UdfFunction9[
      -T1: TypeTag,
      -T2: TypeTag,
      -T3: TypeTag,
      -T4: TypeTag,
      -T5: TypeTag,
      -T6: TypeTag,
      -T7: TypeTag,
      -T8: TypeTag,
      -T9: TypeTag,
      +R: TypeTag
  ](fn: Function9[T1, T2, T3, T4, T5, T6, T7, T8, T9, R]) {
    def udf: UserDefinedFunction = functions.udf(fn)

    /**
     * @param outType forced output schema
     * @return
     */
    def udf(outType: DataType): UserDefinedFunction = functions.udf(fn, outType)
  }

  implicit class UdfFunction10[
      -T1: TypeTag,
      -T2: TypeTag,
      -T3: TypeTag,
      -T4: TypeTag,
      -T5: TypeTag,
      -T6: TypeTag,
      -T7: TypeTag,
      -T8: TypeTag,
      -T9: TypeTag,
      -T10: TypeTag,
      +R: TypeTag
  ](fn: Function10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, R]) {
    def udf: UserDefinedFunction = functions.udf(fn)

    /**
     * @param outType forced output schema
     * @return
     */
    def udf(outType: DataType): UserDefinedFunction = functions.udf(fn, outType)
  }
}

object UdfUtils extends UdfUtils
