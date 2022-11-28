package learning.spark.rowAndcolumnTransformations

import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

//VM args : -Dlog4j.configuration=file:log4j.properties -Dlogfile.name=appName -Dspark.yarn.app.container.log.dir=app-logs
//enable shorten command line path in run configuation with either jar/classpath
object DataFrameRowTransformations extends Serializable {

  def main(args: Array[String]): Unit = {
    @transient val logger: Logger = Logger.getLogger(getClass.getName)
    logger.info("Starting DataFrameRowTransformations")

    val spark = SparkSession.builder()
      .appName("DataFrameRowTransformations")
      .master("local[3]")
      .getOrCreate()

    val schema = StructType(List(
      StructField("ID",StringType),
      StructField("EventTime",StringType)
    ))

    val rows = List(Row("123","04/05/2020"),Row("123","4/5/2020"),Row("123","04/5/2020"),Row("123","4/05/2020"))
    val rdd = spark.sparkContext.parallelize(rows,2);
    val df = spark.createDataFrame(rdd,schema)

    df.printSchema()
    df.show()

    val newDF = toDateDF(df,"M/d/y","EventTime")
    newDF.printSchema()
    newDF.show()
  }

  def toDateDF(df:DataFrame,fmt:String,colName:String):DataFrame={
    df.withColumn(colName,to_date(col(colName),fmt))
  }

}
