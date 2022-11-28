package learning.spark.rowAndcolumnTransformations

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

//VM args : -Dlog4j.configuration=file:log4j.properties -Dlogfile.name=appName -Dspark.yarn.app.container.log.dir=app-logs
//enable shorten command line path in run configuation with either jar/classpath
object DataFrameColumnTransformations extends Serializable {
  def main(args: Array[String]): Unit = {
    @transient val logger: Logger = Logger.getLogger(getClass.getName)
    logger.info("Starting DataFrameColumnTransformations")

    val spark = SparkSession.builder()
      .appName("DataFrameColumnTransformations")
      .master("local[3]")
      .getOrCreate()

    val flightTimeParquetDF = spark.read
      .format("parquet")
      .option("path", "data/flight*.parquet")
      .load()
    //referencing columns using Column String
    //flightTimeParquetDF.select("OP_CARRIER").show(10)

    //referencing columns using column object
    import org.apache.spark.sql.functions._
    import spark.implicits._
    //flightTimeParquetDF.select(column("OP_CARRIER"), col("ORIGIN"), $"DEST", 'DISTANCE ).show()


    //adding a column name year by parsing date column
    //flightTimeParquetDF.withColumn("year", year(to_date($"FL_DATE", "M/d/y")))
    //newDF.show(5)

    //using string expressions
    //flightTimeParquetDF.selectExpr("OP_CARRIER", "FL_DATE","year(to_date(FL_DATE, \"M/d/y\")) as year").show(5)

    //using col object expression
    flightTimeParquetDF.select($"OP_CARRIER", $"FL_DATE",year(to_date($"FL_DATE", "M/d/y")) as "Year").show(5)


    spark.stop()

  }
}
