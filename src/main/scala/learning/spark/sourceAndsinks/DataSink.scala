package learning.spark.sourceAndsinks

import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}

//VM args : -Dlog4j.configuration=file:log4j.properties -Dlogfile.name=appName -Dspark.yarn.app.container.log.dir=app-logs
//enable shorten command line path in run configuation with either jar/classpath
object DataSink extends Serializable {

  def main(args: Array[String]): Unit = {
    @transient val logger: Logger = Logger.getLogger(getClass.getName)
    logger.info("Starting DataSink")

    val spark = SparkSession.builder()
      .appName("DataSink")
      .master("local[3]")
      .getOrCreate()

    val flightTimeParquetDF = spark.read
      .format("parquet")
      .option("path", "data/flight*.parquet")
      .load()

    logger.info("number of partitions : "+flightTimeParquetDF.rdd.getNumPartitions)
    import org.apache.spark.sql.functions.spark_partition_id
    flightTimeParquetDF.groupBy(spark_partition_id()).count().show()
    /*
        val partitionDF = flightTimeParquetDF.repartition(5)
        logger.info("number of partitions post repartitions: "+partitionDF.rdd.getNumPartitions)
        partitionDF.groupBy(spark_partition_id()).count().show()


        partitionDF.write
          .format("avro")
          .mode(SaveMode.Overwrite)
          .option("path","datasink/avro/")
          .save()
     */
    flightTimeParquetDF.write
      .format("json")
      .mode(SaveMode.Overwrite)
      .option("path", "datasink/json/")
      .partitionBy("OP_CARRIER","ORIGIN")
      .save()

  }

}
