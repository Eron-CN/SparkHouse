package com.hibbo.bankDemo.pipeline

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.Logger

trait DataPipeline {

  def run(argsMap: Map[String, String]): Unit = {
    val pipelineName = argsMap.getOrElse("--PipelineName".toLowerCase, "")
    var sparkSession: Option[SparkSession] = None
    try {
      sparkSession = Some(initialize(pipelineName))
      explore(sparkSession.get, argsMap)
    } catch {
      case e: Exception => throw e
    } finally {
      sparkSession match {
        case Some(s) => s.stop()
      }
    }
  }

  def read(spark: SparkSession, rawInputPath: String): DataFrame

  def explore(sparkSession: SparkSession, argsMap: Map[String, String]): Unit

  def output(sparkSession: SparkSession,
             dataFrame: DataFrame,
             format: String,
             outputPath: String): Unit

  def initialize(pipelineName: String): SparkSession ={
    SparkSession
      .builder()
      .appName(pipelineName)
      .getOrCreate()
  }


}
