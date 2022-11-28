package learning.spark.rowAndcolumnTransformations

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession


case class ApacheLogRecord(ip: String, date: String, request: String, referrer: String)

//VM args : -Dlog4j.configuration=file:log4j.properties -Dlogfile.name=appName -Dspark.yarn.app.container.log.dir=app-logs
//enable shorten command line path in run configuation with either jar/classpath

object LogFileParseRows extends Serializable {

  def main(args: Array[String]): Unit = {
    @transient val logger: Logger = Logger.getLogger(getClass.getName)
    logger.info("Starting LogFileParseRows")

    val spark = SparkSession.builder()
      .appName("LogFileParseRows")
      .master("local[3]")
      .getOrCreate()

    val df = spark.read.textFile("data/apache_logs.txt").toDF()

    val myReg = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)"""".r

    import spark.implicits._
    val logDF = df.map(row =>
      row.getString(0) match {
        case myReg(ip, client, user, date, cmd, request, proto, status, bytes, referrer, userAgent) =>
          ApacheLogRecord(ip, date, request, referrer)
      }
    )

    import org.apache.spark.sql.functions._
    logDF
      .where("trim(referrer) != '-' ")
      .withColumn("referrer", substring_index($"referrer", "/", 3))
      .groupBy("referrer").count().show(truncate = false)
    spark.stop()

  }

}
