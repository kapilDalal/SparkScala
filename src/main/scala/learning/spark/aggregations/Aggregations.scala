package learning.spark.aggregations

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object Aggregations {
    def main(args: Array[String]): Unit = {
      @transient val logger: Logger = Logger.getLogger(getClass.getName)
      logger.info("Starting Aggregations")

      val spark = SparkSession.builder()
        .appName("Aggregations")
        .master("local[3]")
        .getOrCreate()

      val df = spark.read
        .format("csv")
        .option("header","true")
        .option("inferSchema","true")
        .option("path","data/invoices.csv")
        .load()
/*

      //Simple aggregation with column object expressions
      df.select(
        count("*").as("Count*"),
        sum("Quantity").as("TotalQuantity"),
        avg("UnitPrice").as("AvgPrice"),
        countDistinct("InvoiceNo").as("CountDistinct")
      ).show()

      //Simple aggregation with sql like expressions
      df.selectExpr(
        "count(1) as `count 1`",
        "count(StockCode) as `count field`",
        "sum(Quantity) as TotalQuantity",
        "avg(UnitPrice) as AvgPrice"
      ).show()
*/

      //Grouping aggregates
      df.createOrReplaceTempView("sales")
      val summarySQL = spark.sql(
        """
          |SELECT Country, InvoiceNo,
          | sum(Quantity) as TotalQuantity,
          | round(sum(Quantity*UnitPrice),2) as InvoiceValue
          | FROM sales
          | GROUP BY Country, InvoiceNo
          |""".stripMargin)

      summarySQL.show()



    }
}
