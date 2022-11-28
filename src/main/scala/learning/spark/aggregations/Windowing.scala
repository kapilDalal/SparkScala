package learning.spark.aggregations

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object Windowing {

  def main(args: Array[String]): Unit = {
    @transient val logger: Logger = Logger.getLogger(getClass.getName)
    logger.info("Starting Windowing")

    val spark = SparkSession.builder()
      .appName("Windowing")
      .master("local[3]")
      .getOrCreate()

    val df = spark.read
      .format("parquet")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("path", "data/summary.parquet")
      .load()

    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions._

    val runningWindow = Window.partitionBy("Country")
      .orderBy("WeekNumber")
      .rowsBetween(Window.unboundedPreceding,Window.currentRow)

    df.withColumn(
      "RunningTotal",sum("InvoiceValue").over(runningWindow)
    ).show()

    /*
     val rankWindow = Window.partitionBy("Country")
      .orderBy(col("InvoiceValue").desc)
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    summaryDF.withColumn("Rank", dense_rank().over(rankWindow))
      .where(col("Rank") ===1)
      .sort("Country", "WeekNumber")
      .show()
     */

  }

}
