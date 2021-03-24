package com.hibbo.sparkhouse

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import java.io.File

class SparkFunSuite extends FunSuite with BeforeAndAfterAll{
  var spark: SparkSession = _
  var dummyDf: DataFrame  = _

  override protected def beforeAll(): Unit = {
    workaroundForWinUtilOnWindows()

    spark = SparkSession
      .builder()
      .appName(getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    dummyDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq("a", "b")).map(row => Row(row)),
      StructType(List(StructField("name", StringType)))
    )
  }

  def setSparkConf(): Unit = {
    spark.sparkContext.setLogLevel( "ERROR" )
  }

  override protected def afterAll(): Unit = {
    spark.stop()
  }

  // workaround for spark running on windows bug
  def workaroundForWinUtilOnWindows(): Unit = {
    val workaround = new File(".")
    System.getProperties.put("hadoop.home.dir", workaround.getAbsolutePath)
  }
}
