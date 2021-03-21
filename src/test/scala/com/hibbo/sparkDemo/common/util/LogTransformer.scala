package com.hibbo.sparkDemo.common.util

import com.hibbo.sparkDemo.SparkFunSuite
import org.apache.spark.sql.{SaveMode, SparkSession}

class LogTransformer extends SparkFunSuite{
  test("test LogTransformer"){
    val spark = SparkSession
      .builder()
      .appName(getClass.getSimpleName)
      .getOrCreate()

    val input = "/raw/Compare/ReplyPair/HDI_ADLA_Compare_20201111_RP.log"
    val output = "/csv/Compare/ReplyPair/20201111"

    import spark.implicits._
    val dataFrame = spark.read
      .textFile(getFilePath(input))
      .map(_.split(" "))
      .map(v => (v(6), v(8)))
      .toDF("metrics", "value")

    dataFrame.write
      .mode(SaveMode.Overwrite)
      .csv(getFilePath(output))

    spark.stop()
  }

  def getFilePath(outputPath: String): String ={
    getClass
      .getResource(outputPath)
      .getPath
  }
}
