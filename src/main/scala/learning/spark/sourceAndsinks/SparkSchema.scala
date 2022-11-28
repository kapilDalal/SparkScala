package learning.spark.sourceAndsinks

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

//VM args : -Dlog4j.configuration=file:log4j.properties -Dlogfile.name=appName -Dspark.yarn.app.container.log.dir=app-logs
//enable shorten command line path in run configuation with either jar/classpath
object SparkSchema extends Serializable {


  def main(args: Array[String]): Unit = {
    @transient val logger: Logger = Logger.getLogger(getClass.getName)
    logger.info("Starting SparkSchema")

    val spark = SparkSession.builder()
      .appName("SparkSchema")
      .master("local[3]")
      .getOrCreate()

    /*
//checking if inferSchema works the way we expect
    val flightTimeCsvDF = spark.read
      .format("csv")
      .option("header","true")
      .option("path","data/flight*.csv")
      .load()

    //flightTimeCsvDF.show(5)
    //logger.info(flightTimeCsvDF.schema.simpleString)

    val flightTimeCsvWithInferSchemaDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema","true")
      .option("path", "data/flight*.csv")
      .load()

    //logger.info(flightTimeCsvWithInferSchemaDF.schema.simpleString)

    val flightTimeJsonDF = spark.read
      .format("json")
      //.option("header", "true")
      .option("path", "data/flight*.json")
      .load()
    //logger.info(flightTimeJsonDF.schema.simpleString)


    val flightTimeParquetDF = spark.read
      .format("parquet")
      //.option("header", "true")
      .option("path", "data/flight*.parquet")
      .load()
    logger.info(flightTimeParquetDF.schema.simpleString)
 */


//Assign explicit schema

    //assigning explicit schema programmatically
    val fligtSchemaStruct = StructType(List(
      StructField("FL_DATE", DateType),
      StructField("OP_CARRIER", StringType),
      StructField("OP_CARRIER_FL_NUM", IntegerType),
      StructField("ORIGIN", StringType),
      StructField("ORIGIN_CITY_NAME", StringType),
      StructField("DEST", StringType),
      StructField("DEST_CITY_NAME", StringType),
      StructField("CRS_DEP_TIME", IntegerType),
      StructField("DEP_TIME", IntegerType),
      StructField("WHEELS_ON", IntegerType),
      StructField("TAXI_IN", IntegerType),
      StructField("CRS_ARR_TIME", IntegerType),
      StructField("ARR_TIME", IntegerType),
      StructField("CANCELLED", IntegerType),
      StructField("DISTANCE", IntegerType)
    ))

    val flightSchemaDDL = "FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING, " +
      "ORIGIN_CITY_NAME STRING, DEST STRING, DEST_CITY_NAME STRING, CRS_DEP_TIME INT, DEP_TIME INT, " +
      "WHEELS_ON INT, TAXI_IN INT, CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED INT, DISTANCE INT"


    val flightTimeCsvDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("path", "data/flight*.csv")
      .option("mode","FAILFAST")
      .option("dateFormat","M/d/y")
      .schema(fligtSchemaStruct)
      .load()

    //flightTimeCsvDF.show(5)

    //logger.info("csv schema: "+flightTimeCsvDF.schema.mkString)


    val flightTimeJsonDF = spark.read
      .format("json")
      //.option("header", "true")
      .option("path", "data/flight*.json")
      .option("dateFormat","M/d/y")
      .schema(flightSchemaDDL)
      .load()
    flightTimeJsonDF.show(5)
    logger.info(flightTimeJsonDF.schema.simpleString)

    spark.stop()
  }

}
