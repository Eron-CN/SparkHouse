package com.hibbo.sparkDemo.pipeline

import com.hibbo.sparkDemo.common.PipelineContext
import com.hibbo.sparkDemo.common.utils.CommonUtil
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.hibbo.sparkDemo.transform.TransformerBase
import org.apache.log4j.Logger
import org.apache.spark.storage.StorageLevel

trait DataPipeline extends TransformerBase {

  private val logger = Logger.getLogger(getClass.getName)

  def run(argsMap: Map[String, String]): Unit = {
    val pipelineName = argsMap.getOrElse("--PipelineName".toLowerCase, "")
    var sparkSession: Option[SparkSession] = None
    try {
      sparkSession = Some(initialize(pipelineName))
      handle(sparkSession.get, argsMap)
    } catch {
      case e: Exception => throw e
    } finally {
      sparkSession match {
        case Some(s) => s.stop()
      }
    }
  }

  def read(spark: SparkSession, rawInputPath: String): DataFrame

  def handle(sparkSession: SparkSession, argsMap: Map[String, String]): Unit = {
    val rawInputPath                  = argsMap("--FilePath".toLowerCase)
    val persistLevelStr               = argsMap.getOrElse("--PersistLevel".toLowerCase, "")
    val extensionProperties           = argsMap.getOrElse("--ExtensionProperties".toLowerCase, "") //The input format is "a->b,c->d"
    val outputPath                    = argsMap("--OutputPath".toLowerCase)

    val persistLevel = getPersistLevel(persistLevelStr)
    val context = enrichExtensionProperties(extensionProperties)

    // 1. read raw data to df/ds
    val rawDF = read(sparkSession, rawInputPath)
    // 2. transform data
    val transformDF = transform(sparkSession, rawDF, persistLevel, context)
    // 3. output data
    output(sparkSession, transformDF, "parquet", outputPath)
  }

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

  def getPersistLevel(level: String): StorageLevel = {
    var storageLevel = StorageLevel.MEMORY_AND_DISK
    try {
      storageLevel = StorageLevel.fromString(level.toUpperCase)
    } catch {
      case _: Throwable =>
        logger.warn(s"Persist Level $level not found! Use default MEMORY_AND_DISK")
    }
    storageLevel
  }

  def enrichExtensionProperties(extensionProperties: String): PipelineContext = {
    if(!CommonUtil.isNullOrEmpty(extensionProperties)){
      val map = extensionProperties
        .split(",")
        .filter(_.trim != "")
        .map(item => (item.split("->")(0).trim, item.split("->")(1).trim))
        .toMap
      new PipelineContext(map)
    } else {
      null
    }
  }
}
