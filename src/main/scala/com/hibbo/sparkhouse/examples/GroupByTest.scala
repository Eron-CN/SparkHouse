package com.hibbo.sparkhouse.examples

import org.apache.spark.sql.SparkSession

import scala.util.Random

object GroupByTest {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession
      .builder()
      .appName("GroupByTest")
      .getOrCreate()

    val numMappers  = args(0).toInt // 100
    val numKVPairs  = args(1).toInt // 10000
    val valSize     = args(2).toInt // 1000
    val numReducers = args(3).toInt // 36

    val pairs = sparkSession.sparkContext.parallelize(0 until numMappers, numMappers).flatMap { p =>
      val ranGen = new Random()
      val kvPairs = new Array[(Int, Array[Byte])](numKVPairs)
      for (i <- 0 until numKVPairs) {
        // [0, 0, 0, 0, 0...]
        val bytesArr = new Array[Byte](valSize)
        // [10, 12, 27, -5, 119...]
        ranGen.nextBytes(bytesArr)
        kvPairs(i) = (ranGen.nextInt(Int.MaxValue), bytesArr)
      }
      kvPairs
    }

  }
}
