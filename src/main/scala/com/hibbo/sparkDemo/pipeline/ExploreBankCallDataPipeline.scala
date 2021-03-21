package com.hibbo.sparkDemo.pipeline

import com.hibbo.sparkDemo.common.PipelineContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

class ExploreBankCallDataPipeline extends DataPipeline {
  override def run(argsMap: Map[String, String]): Unit = ???

  override def read(spark: SparkSession, rawInputPath: String): DataFrame = ???

  override def handle(sparkSession: SparkSession, argsMap: Map[String, String]): Unit = ???

  override def output(sparkSession: SparkSession, dataFrame: DataFrame, format: String, outputPath: String): Unit = ???

  override def transform(sparkSession: SparkSession, dataFrame: DataFrame, persistLevel: StorageLevel, context: PipelineContext): DataFrame = ???
}
