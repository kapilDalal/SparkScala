package learning.spark.joins

import org.apache.spark.sql.SparkSession

object JoinInternals {


    def main(args:Array[String]):Unit={

      // we will have 3 parallel threads to simulate spark cluster executors
      val spark = SparkSession.builder()
        .master("local[3]")
        .appName("JoinInternals")
        .getOrCreate()

      // there are 3 files in these folders, so this should be read as 3 partitions
      // we have made sure that we get 3 executors and 3 partitions for these data frames
      val df1 = spark.read.json("data/d1/")
      val df2 = spark.read.json("data/d2")

      // now, we will set the shuffle partition, this will ensure that we get 3 partitions post shuffle
      // which means we will have 3 reduce exchanges
      spark.conf.set("spark.sql.shuffle.partitions",3)

      val joinExpr = df1.col("id")===df2.col("id")

      val joinDF = df1.join(df2,joinExpr,"inner")

      joinDF.foreach(_=>())
      scala.io.StdIn.readLine()


    }



}
