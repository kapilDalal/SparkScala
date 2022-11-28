package learning.spark.rowAndcolumnTransformations

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

//VM args : -Dlog4j.configuration=file:log4j.properties -Dlogfile.name=appName -Dspark.yarn.app.container.log.dir=app-logs
//enable shorten command line path in run configuation with either jar/classpath
object MiscellaneousTransformations {

  def main(args: Array[String]): Unit = {
    @transient val logger: Logger = Logger.getLogger(getClass.getName)
    logger.info("Starting MiscellaneousTransformations")

    val spark = SparkSession.builder()
      .appName("MiscellaneousTransformations")
      .master("local[3]")
      .getOrCreate()

    //short way to create DF
    val list = List(
      ("abc",28,1,2002),
      ("def",23,1,81),
      ("ghi",12,12,6),
      ("jkl",7,8,63),
      ("def",23,1,81)
    )
    val df = spark.createDataFrame(list).toDF("name","day","month","year").repartition(3)
      .withColumn("day",col("day").cast(IntegerType))
    //df.show()

    //using switch case
    val df1 = df.withColumn("year",expr(
      """
        case when year<=30 then year + 2000
        when year>30 and year<100 then year + 1900
        else year
        end"""))
    //df1.show()

    val df2 = df.withColumn("year",
      when(col("year")<=30,col("year")+2000)
      when(col("year")>30 and col("year")<100,col("year")+1900)
      otherwise(col("year")))
    //df2.show()

    val df3 = df2.withColumn("dob",expr("concat(year,'/',month,'/',day) as dob"))
      .drop("year","month","day")
      .dropDuplicates("name","dob")
    df3.show()

  }
}
