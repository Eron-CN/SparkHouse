package com.hibbo.bankDemo.pipeline

import org.apache.spark.sql.{DataFrame, SparkSession}

class ExploreBankCallDataPipeline extends DataPipeline {
  override def run(argsMap: Map[String, String]): Unit = {
    // init job context

  }

  override def read(spark: SparkSession, rawInputPath: String): DataFrame = ???

  override def explore(sparkSession: SparkSession, argsMap: Map[String, String]): Unit = ???

  override def output(sparkSession: SparkSession, dataFrame: DataFrame, format: String, outputPath: String): Unit = ???
}
