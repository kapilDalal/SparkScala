package learning.spark.rddAnddatasetAndsql

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
//VM args : -Dlog4j.configuration=file:log4j.properties -Dlogfile.name=appName -Dspark.yarn.app.container.log.dir=app-logs
//enable shorten command line path in run configuation with either jar/classpath
//program args : flight-time.parquet
object SparkSQL extends Serializable {

  def main(args: Array[String]): Unit = {
    @transient val logger: Logger = Logger.getLogger(getClass.getName)
    logger.info("Starting SparkSQL")

    if (args.length == 0) {
      logger.error("no argument provided")
      System.exit(1)
    }
    val spark = SparkSession.builder()
      .appName("SparkSQL")
      .master("local[3]")
      .getOrCreate();

    val surveyDF = spark.read
      .option("inferSchema", true)
      .option("header", true)
      .csv(args(0))

    surveyDF.createOrReplaceTempView("temp_view")

    val countDF = spark.sql("select Country,count(1) as count from temp_view where Age<40 group by Country")

    logger.info(countDF.collect().mkString)

  }
}
