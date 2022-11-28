package learning.spark.createdf

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties
import scala.io.Source

//VM args : -Dlog4j.configuration=file:log4j.properties -Dlogfile.name=appName -Dspark.yarn.app.container.log.dir=app-logs
//enable shorten command line path in run configuation with either jar/classpath
////program args : samplewithheader.csv
object HelloWorld{

  @transient val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    logger.info("starting application")

    if(args.length==0){
      logger.error("no argument provided")
      System.exit(1)
    }

    val spark = SparkSession.builder()
      .config(getSparkAppConf)
      .getOrCreate()

    val df = loadDF(spark,args(0))

    val partitionDF = df.repartition(2)

    val countDF = countByCountry(partitionDF)
    //countDF.show()
    logger.info(countDF.collect().mkString)
    //logger.info("spark.conf="+spark.conf.getAll.toString())
    scala.io.StdIn.readLine()
    spark.stop()
    logger.info("done with application")

  }

  def countByCountry(dataFrame: DataFrame):DataFrame={
      dataFrame.filter("Age<40")
        .select("Age", "Gender", "Country", "state")
        .groupBy("country")
        .count()
  }

  def loadDF(spark:SparkSession,file:String):DataFrame={
    val df = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv(file)
    df
  }

  def getSparkAppConf:SparkConf={
    val sparkConf = new SparkConf()
    val properties = new Properties()
    properties.load(Source.fromFile("spark.conf").bufferedReader())
    properties.forEach((k,v)=>sparkConf.set(k.toString,v.toString))
    sparkConf
  }


}
