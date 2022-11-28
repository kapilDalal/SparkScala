package learning.spark.joins

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object SparkJoin {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark Join Demo")
      .master("local[3]")
      .getOrCreate()

    val ordersList = List(
      ("01", "02", 350, 1),
      ("01", "04", 580, 1),
      ("01", "07", 320, 2),
      ("02", "03", 450, 1),
      ("02", "06", 220, 1),
      ("03", "01", 195, 1),
      ("04", "09", 270, 3),
      ("04", "08", 410, 2),
      ("05", "02", 350, 1)
    )
    val orderDF = spark.createDataFrame(ordersList).toDF("order_id", "prod_id", "unit_price", "qty")

    val productList = List(
      ("01", "Scroll Mouse", 250, 20),
      ("02", "Optical Mouse", 350, 20),
      ("03", "Wireless Mouse", 450, 50),
      ("04", "Wireless Keyboard", 580, 50),
      ("05", "Standard Keyboard", 360, 10),
      ("06", "16 GB Flash Storage", 240, 100),
      ("07", "32 GB Flash Storage", 320, 50),
      ("08", "64 GB Flash Storage", 430, 25)
    )
    val productDF = spark.createDataFrame(productList).toDF("prod_id", "prod_name", "list_price", "qty")

    //inner join : if the condition is not matched the records will be dropped
    val joinExpr = orderDF.col("prod_id") === productDF.col("prod_id")
    val joinType = "inner"

    //orderDF.join(productDF, joinExpr, joinType).show()
    //if we select qty then there will be an ambiguity which we can resolve by renaming col or dropping it


    //outer join
    /*orderDF.join(productDF,joinExpr,"outer")
      .sort("Order_id")
      .show()*/

    //above outer join was also showing the data which we have not sold but we need only order data
    //so we can use left join
    import org.apache.spark.sql.functions._

    val productRenamedDF = productDF.withColumnRenamed("qty","reorder_qty")
    orderDF.join(productRenamedDF,joinExpr,"left")
      .drop(productRenamedDF.col("prod_id"))
      .select("order_id","prod_id","prod_name","unit_price","list_price","qty")
      .withColumn("prod_name",expr("coalesce(prod_name,prod_id)"))
      .sort("order_id")
      .show()


  }

}
