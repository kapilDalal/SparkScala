package learning.spark

import learning.spark.MergeCalmData.getClass
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object testclass {

  def main(args:Array[String]):Unit={
    @transient val logger: Logger = Logger.getLogger(getClass.getName)
    logger.info("Starting testclass")

    val spark = SparkSession.builder()
      .appName("testclass")
      .master("local[3]")
      .getOrCreate()

    val df = spark.read
      .format("parquet")
      .option("path","data/CertificateInfo.parquet")
      .load()

    println(df.schema.mkString("=>"))

    /*val df1 = spark.read
      .format("parquet")
      .option("path", "daily/CertificateInfo_2022_10_31.parquet")
      .load()

    println(df1.count())*/
  }

}
