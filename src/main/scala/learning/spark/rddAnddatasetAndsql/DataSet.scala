package learning.spark.rddAnddatasetAndsql

import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, SparkSession}


case class  SurveyRecordDataset(Age:Int,Gender:String,Country:String,state:String)

//VM args : -Dlog4j.configuration=file:log4j.properties -Dlogfile.name=appName -Dspark.yarn.app.container.log.dir=app-logs
//enable shorten command line path in run configuation with either jar/classpath
//program args : samplewithheaders.csv
object DataSet extends Serializable {

  def main(args:Array[String]):Unit={
    @transient val logger: Logger = Logger.getLogger(getClass.getName)
    logger.info("Starting DataSet")

    if (args.length == 0) {
      logger.error("no argument provided")
      System.exit(1)
    }
    val spark = SparkSession.builder()
      .appName("Dataset")
      .master("local[3]")
      .getOrCreate();

    val surveyDF = spark.read
      .option("inferSchema",true)
      .option("header",true)
      .csv(args(0))

    import spark.implicits._

    val surveyDS:Dataset[SurveyRecordDataset] = surveyDF.select("Age","Gender","Country","state").as[SurveyRecordDataset]
    logger.info(surveyDS.collect().mkString)
    val filteredDS = surveyDS.filter(row=>row.Age<40) // compile time safety
    val filteredDF = surveyDF.filter("Age<40") //run time safety

    //type safe GroupBy
    filteredDS.groupByKey(r=>r.Country).count()
    //Runtime GroupBy
    filteredDF.groupBy("Country").count()




  }

}
