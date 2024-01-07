# spark-utils
Spark helper methods for Data Analysts and Data Scientists 


## Column Category Macros

```scala
// quasi exponential distribtion generator
def randExp(lambda: Double = 1.0): Column = lit(lambda) * exp(lit(-1 * lambda) * ( randn() + lit(3.71)))

// testing Dataset
val expDF = spark
  .range(10000)
  .withColumn("value", lit(200) * randExp())
  
  
expDF
  .groupBy(
     $"value"
       .STEP_CATEGORY(20)                            // using steps of size 20
       .as("step_cat")
  ).count
  
expDF
  .groupBy(
     $"value"
       .LOG_CATEGORY(scala.math.E, castTo="Double")  // using exponentially increasing steps
       .round(3)                                     // round category values to a given degree of accuracy
       .as("log_cat")
  ).count
  
expDF  
  .groupBy(
     $"value"
       .INTERVAL_CATEGORY(Seq(0, 1, 5, 25, 125))     // using user defined steps
       .as("interval_cat")
  ).count
  
 //////////////////// out: /////////////////////////////////////////
 //  +--------+-----+      +-------+-----+      +------------+-----+  
 //  |step_cat|count|      |log_cat|count|      |interval_cat|count|
 //  +--------+-----+      +-------+-----+      +------------+-----+
 //  |0.0     |9198 |      |0.135  |1    |      |0.0         |588  |
 //  |20.0    |617  |      |0.368  |58   |      |1.0         |4477 |
 //  |40.0    |120  |      |1.0    |2670 |      |5.0         |4439 |
 //  |60.0    |36   |      |2.718  |3910 |      |25.0        |490  |
 //  |80.0    |18   |      |7.389  |2570 |      |125.0       |6    |
 //  |100.0   |7    |      |20.086 |710  |      +------------+-----+
 //  |120.0   |1    |      |54.598 |75   |
 //  |240.0   |1    |      |148.413|6    |
 //  |260.0   |1    |      +-------+-----+
 //  |320.0   |1    |
 //  +--------+-----+
```

## Date Column formatting
```scala
import java.util.Locale
val dateAdd: (java.sql.Date, Int) => java.sql.Date = (dt, i) => java.sql.Date.valueOf(dt.toLocalDate.plusDays(i))

spark
  .range(10)
  .select(
     dateAdd
       .udf(lit("2023-09-30"), col("id"))
       .as("date")
  )
  .withColumn("fmt_week",    formatDate("yyyy-'W'w", Locale.UK).udf($"date"))
  .withColumn("fmt_month",   formatDate(  "yyyy-MM"           ).udf($"date"))
  .withColumn("fmt_quarter", formatDate("yyyy-'Q'Q"           ).udf($"date"))
  .withColumn("fmt_dow",     formatDate(        "E", Locale.UK).udf($"date"))

//////////////////// out: /////////////////////////////
//  +----------+--------+---------+-----------+-------+
//  |      date|fmt_week|fmt_month|fmt_quarter|fmt_dow|
//  +----------+--------+---------+-----------+-------+
//  |2023-09-30|2023-W39|  2023-09|    2023-Q3|    Sat|
//  |2023-10-01|2023-W39|  2023-10|    2023-Q4|    Sun|
//  |2023-10-02|2023-W40|  2023-10|    2023-Q4|    Mon|
//  |2023-10-03|2023-W40|  2023-10|    2023-Q4|    Tue|
//  |2023-10-04|2023-W40|  2023-10|    2023-Q4|    Wed|
//  |2023-10-05|2023-W40|  2023-10|    2023-Q4|    Thu|
//  |2023-10-06|2023-W40|  2023-10|    2023-Q4|    Fri|
//  |2023-10-07|2023-W40|  2023-10|    2023-Q4|    Sat|
//  |2023-10-08|2023-W40|  2023-10|    2023-Q4|    Sun|
//  |2023-10-09|2023-W41|  2023-10|    2023-Q4|    Mon|
//  +----------+--------+---------+-----------+-------+
```