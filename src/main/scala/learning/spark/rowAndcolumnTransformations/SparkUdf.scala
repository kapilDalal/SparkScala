package learning.spark.rowAndcolumnTransformations

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
//VM args : -Dlog4j.configuration=file:log4j.properties -Dlogfile.name=appName -Dspark.yarn.app.container.log.dir=app-logs
//enable shorten command line path in run configuation with either jar/classpath
object SparkUdf extends Serializable {

  def main(args: Array[String]): Unit = {
    @transient val logger: Logger = Logger.getLogger(getClass.getName)
    logger.info("Starting DataSink")

    val spark = SparkSession.builder()
      .appName("DataSink")
      .master("local[3]")
      .getOrCreate()

    val df = spark.read
      .format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .option("path", "data/survey.csv")
      .load()

    import org.apache.spark.sql.functions._

    //register as DF UDF
    val parseGenderUdf = udf(parseGender(_:String):String)
    spark.catalog.listFunctions().filter(r=>r.name=="parseGenderUDF").show()
    val newDF = df.withColumn("Gender",parseGenderUdf(col("Gender")))
    newDF.show(10,truncate = false)

    //register as sql UDF
    spark.udf.register("parseGenderUDF1",parseGender(_:String):String)
    spark.catalog.listFunctions().filter(r=>r.name=="parseGenderUDF1").show()
    val newDF1 = df.withColumn("Gender", expr("parseGenderUdf1(Gender)"))
    newDF1.show(10, truncate = false)
  }

  def parseGender(s:String):String={
    val femalePattern = "^f$|f.m|w.m".r
    val malePattern = "^m$|ma|m.l".r

    if(femalePattern.findAllMatchIn(s.toLowerCase()).nonEmpty) "Female"
    else if(malePattern.findAllMatchIn(s.toLowerCase()).nonEmpty) "Male"
    else "Unknown"
  }
}
