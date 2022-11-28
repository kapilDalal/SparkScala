package learning.spark.rddAnddatasetAndsql

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

case class SurveyRecord(Age:Int,Gender:String,Country:String,state:String)

//VM args : -Dlog4j.configuration=file:log4j.properties -Dlogfile.name=appName -Dspark.yarn.app.container.log.dir=app-logs
//enable shorten command line path in run configuation with either jar/classpath
//program args : sample.csv
object RDD extends Serializable{


  def main(args: Array[String]): Unit = {

    @transient lazy val logger:Logger = Logger.getLogger(getClass.getName)

    if(args.length==0){
      logger.error("no args")
      System.exit(1)
    }

    val sparkConf = new SparkConf()
      .setAppName("RDD")
      .setMaster("local[3]");

    val sparkContext = new SparkContext(sparkConf);
    val linesRDD = sparkContext.textFile(args(0))
    val partitionRDD = linesRDD.repartition(2)
    val colsRDD = partitionRDD.map(lines=>lines.split(",").map(_.trim))
    val selectRDD = colsRDD.map(cols=>SurveyRecord(cols(1).toInt,cols(2),cols(3),cols(4)))
    val filteredRDD = selectRDD.filter(row => row.Age<40)

    val kvRDD = filteredRDD.map(rows=>(rows.Country,1))
    val countRDD = kvRDD.reduceByKey((v1,v2)=>v1+v2)

    logger.info(countRDD.collect().mkString("=>"))

  }



}
