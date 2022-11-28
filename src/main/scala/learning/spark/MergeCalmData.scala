package learning.spark
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode, SparkSession}

import java.io.File

object MergeCalmData {


  val uniqueKey = "CI_PartitionKey"
  val orderByKey = "CI_LastUpdated"
  def main(args:Array[String]):Unit={
    @transient val logger: Logger = Logger.getLogger(getClass.getName)
    logger.info("Starting MergeCalmData")

    val spark = SparkSession.builder()
      .appName("MergeCalmData")
      .master("local[3]")
      .getOrCreate()

    val dailyDeltaFilePath = "CertificateInfo/2022/10/31/CertificateInfo_2022_10_31.parquet"
    val dailyFilePath = "CertificateInfo/2022/10/CertificateInfo_2022_10_31"
    val monthlyFilePath = "CertificateInfo/2022/CertificateInfo_2022_10"
    val fileFormat = ".parquet"

    /*import com.azure.storage.common.StorageSharedKeyCredential
    import com.azure.storage.file.datalake._

    import io.netty.handler.logging.LoggingHandler
    val accountName = "adlsgen2painteldev"
    val key = ""
    val creds = new StorageSharedKeyCredential(accountName, key)
    val client = new DataLakeServiceClientBuilder().credential(creds).endpoint("https://" + accountName + ".dfs.core.window.net").buildClient
*/
    //client.getFileSystemClient("integration/PKITA/CertificateInfo/2022/10/31")



    /*
        // Read delta file
        val dailyDeltaFileDF: DataFrame = try {
          spark.read.parquet(dailyDeltaFilePath)
        } catch {
          case e: AnalysisException => {
            println("could not file delta file. returning...")
            null
          }
        }
        dailyDeltaFileDF.show(5)





        //merge with daily file
        val dailyFileDF: DataFrame = try {
          spark.read.parquet(dailyFilePath+fileFormat)
        } catch {
          case e: AnalysisException => {
            dailyDeltaFileDF.write.parquet(dailyFilePath)
            copyParquetInParentDirWithCustomName(dailyFilePath,dailyFilePath+fileFormat)
            deleteFile(dailyFilePath)
            deleteFile(dailyFilePath.toString.split("/")(0)+"/."+dailyFilePath.toString.split("/")(1)+fileFormat+".crc")
            null
          }
        }

        if (dailyFileDF != null) {
          consolidateDataKeepLatest(dailyFileDF,dailyDeltaFileDF,dailyFilePath)
          copyParquetInParentDirWithCustomName(dailyFilePath, dailyFilePath + fileFormat)
          deleteFile(dailyFilePath)
          deleteFile(dailyFilePath.toString.split("/")(0) + "/." + dailyFilePath.toString.split("/")(1) + fileFormat + ".crc")
        }


        //merge with monthly file
        val monthlyFileDF: DataFrame = try{
            spark.read.parquet(monthlyFilePath+fileFormat)
        }catch {
          case e: AnalysisException => {
            val dailyFileDF1 = spark.read.parquet(dailyFilePath+fileFormat)
            dailyFileDF1.write.mode ("overwrite").parquet(monthlyFilePath)
            copyParquetInParentDirWithCustomName(monthlyFilePath, monthlyFilePath + fileFormat)
            deleteFile(monthlyFilePath)
            deleteFile(monthlyFilePath.toString.split("/")(0) + "/." + monthlyFilePath.toString.split("/")(1) + fileFormat + ".crc")
            null
          }
        }

        if(monthlyFileDF!=null){
          consolidateDataKeepLatest(monthlyFileDF, dailyDeltaFileDF, monthlyFilePath)
          copyParquetInParentDirWithCustomName(monthlyFilePath, monthlyFilePath + fileFormat)
          deleteFile(monthlyFilePath)
          deleteFile(monthlyFilePath.toString.split("/")(0) + "/." + monthlyFilePath.toString.split("/")(1) + fileFormat + ".crc")
        }

        */


  }

  def consolidateDataKeepLatest(existingDataDF:DataFrame, newDataDF:DataFrame, existingDataPath:String):Unit= {
    val transformedDailyFileDF = existingDataDF.union(newDataDF).distinct
    val window = Window.partitionBy(uniqueKey).orderBy(desc(orderByKey))
    val filteredDailyDF = transformedDailyFileDF.withColumn("dense_rank", dense_rank().over(window))
      .filter("dense_rank==1")
      .drop("dense_rank")
    filteredDailyDF
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(existingDataPath)
  }

  def consolidateDataDropDuplicates(existingDataDF:DataFrame, newDataDF:DataFrame, existingDataPath:String): Unit ={
    existingDataDF.union(newDataDF)
      .repartition(1)
      .dropDuplicates()
      .write
      .mode(SaveMode.Overwrite)
      .parquet(existingDataPath)
  }

  def deleteFile(path:String): Unit ={
    FileUtil.fullyDelete(new File(path))
  }

  def copyParquetInParentDirWithCustomName(source:String, destFile:String): Unit ={
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    val destPath = new Path(destFile)
    val srcFile = FileUtil.listFiles(new File(source))
      .filter(f => f.getPath.endsWith(".parquet"))(0)
    FileUtil.copy(srcFile, hdfs, destPath, true, hadoopConfig)
  }

}
