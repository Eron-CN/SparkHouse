package com.hibbo.sparkDemo.common.util

import com.hibbo.sparkDemo.SparkFunSuite
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}

class LogCutter extends SparkFunSuite{

  test("test log cut"){

    val spark = SparkSession
      .builder()
      .appName(getClass.getSimpleName)
      .getOrCreate()

    val df = spark
      .read
      .schema(getShcema())
      .text(getPath())

    val df2 = df.sample(0.0001, 2021)

    val count = df2.count()

    println(s"0. count: ${count}")

    df2.write.mode(SaveMode.Overwrite).text(getOutputPath())

    spark.stop()

  }

  test("test read rs"){
    val spark = SparkSession
      .builder()
      .appName(getClass.getSimpleName)
      .getOrCreate()

    val dfNew = spark.read.text(
      getClass
      .getResource("/raw/TopGermanConsumerUserList_2019_09_01_2020_01_31_part")
      .getPath)

    dfNew.show(5)

    spark.stop()

  }

  def getPath(): String ={
    getClass
      .getResource("/raw/ODIN_ML/TopGermanConsumerUserList_2019_09_01_2020_01_31.txt")
      .getPath
  }

  def getOutputPath(): String ={
    getClass
      .getResource("/raw/TopGermanConsumerUserList_2019_09_01_2020_01_31_part")
      .getPath
  }

  def getShcema(): StructType = {
    StructType(Array(StructField("userId", StringType, true)))
  }

}
