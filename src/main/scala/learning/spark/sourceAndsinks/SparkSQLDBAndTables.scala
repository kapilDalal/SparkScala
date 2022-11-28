package learning.spark.sourceAndsinks

import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}

//VM args : -Dlog4j.configuration=file:log4j.properties -Dlogfile.name=appName -Dspark.yarn.app.container.log.dir=app-logs
//enable shorten command line path in run configuation with either jar/classpath
object SparkSQLDBAndTables extends Serializable {

  def main(args: Array[String]): Unit = {
    @transient val logger: Logger = Logger.getLogger(getClass.getName)
    logger.info("Starting SparkSQLDBAndTables")

    val spark = SparkSession.builder()
      .appName("SparkSQLDBAndTables")
      .master("local[3]")
      .enableHiveSupport()
      .getOrCreate()

    val flightTimeParquetDF = spark.read
      .format("parquet")
      .option("path", "data/flight*.parquet")
      .load()

    spark.sql("create database if not exists AIRLINE_DB")
    spark.catalog.setCurrentDatabase("AIRLINE_DB")

    /*flightTimeParquetDF.write
      .mode(SaveMode.Overwrite)
      .partitionBy("ORIGIN","OP_CARRIER")
      .saveAsTable("flight_data")

     */

    flightTimeParquetDF.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      //.partitionBy("ORIGIN", "OP_CARRIER")
      .bucketBy(5,"ORIGIN", "OP_CARRIER")
      .sortBy("ORIGIN", "OP_CARRIER")
      .saveAsTable("flight_data")

    spark.catalog.listTables("AIRLINE_DB").show()
  }

}
