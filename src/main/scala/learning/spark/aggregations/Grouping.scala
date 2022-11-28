package learning.spark.aggregations

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object Grouping {

  def main(args: Array[String]): Unit = {
    @transient val logger: Logger = Logger.getLogger(getClass.getName)
    logger.info("Starting Grouping")

    val spark = SparkSession.builder()
      .appName("Grouping")
      .master("local[3]")
      .getOrCreate()

    val df = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("path", "data/invoices.csv")
      .load()
    //df.show(5)
    import org.apache.spark.sql.functions._

    val numInvoices = countDistinct("InvoiceNo").as("NumInvoices")
    val totalQuantity = sum("Quantity").as("TotalQuantity")
    val invoiceValue = expr("round(sum(Quantity*UnitPrice),2) as InvoiceValue")

    val df1 = df.withColumn("InvoiceDate",to_date(col("InvoiceDate"),"dd-MM-yyyy H.mm"))
      .where("year(InvoiceDate) == 2010")
      .withColumn("WeekNumber",weekofyear(col("InvoiceDate")))
      .groupBy("Country","WeekNumber")
      .agg(numInvoices,totalQuantity,invoiceValue)

    df1.coalesce(1)
      .write
      .format("parquet")
      .option("path","output")
      .mode("overwrite")
      .save()

    df1.sort("Country","WeekNumber").show()



  }

}
