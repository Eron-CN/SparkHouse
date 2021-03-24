package com.hibbo.sparkhouse.pipeline

import com.hibbo.sparkhouse.common.PipelineContext
import com.hibbo.sparkhouse.common.logging.{LogLevel, Logger}
import com.hibbo.sparkhouse.common.utils.CommonUtil
import com.hibbo.sparkhouse.transform.TransformerBase
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

trait DataPipeline extends TransformerBase {

  def run(argsMap: Map[String, String]): Unit = {
    val pipelineName = argsMap.getOrElse("--PipelineName".toLowerCase, "")
    var sparkSession: Option[SparkSession] = None
    try {
      sparkSession = Some(initialize(pipelineName))
      curate(sparkSession.get, argsMap)
    } catch {
      case e: Exception => throw e
    } finally {
      sparkSession match {
        case Some(s) => s.stop()
      }
    }
  }

  def read(spark: SparkSession, rawInputPath: String, fileFormat: String): DataFrame

  def curate(sparkSession: SparkSession, argsMap: Map[String, String]): Unit = {
    val rawInputPath                  = argsMap("--FilePath".toLowerCase)
    val fileFormat                    = argsMap("--InputFileFormat".toLowerCase)
    val outputPath                    = argsMap("--OutputPath".toLowerCase)
    val outputFileFormat              = argsMap("--OutputFileFormat".toLowerCase)
    val persistLevelStr               = argsMap.getOrElse("--PersistLevel".toLowerCase, "")
    val extensionProperties           = argsMap.getOrElse("--ExtensionProperties".toLowerCase, "") //The input format is "a->b,c->d"


    val persistLevel = getPersistLevel(persistLevelStr)
    val context = enrichExtensionProperties(extensionProperties)

    // 1. read raw data to df/ds
    val rawDF = read(sparkSession, rawInputPath, fileFormat)
    // 2. transform data
    val transformDF = transform(sparkSession, rawDF, persistLevel, context)
    // 3. output data
    output(sparkSession, transformDF, outputFileFormat, outputPath)
  }

  def output(spark: SparkSession,
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
        Logger.log(s"Persist Level $level not found! Use default MEMORY_AND_DISK", LogLevel.warn)
    }
    storageLevel
  }

  def enrichExtensionProperties(extensionProperties: String): PipelineContext = {
    val parseExensionProperties = CommonUtil.parseExtensionProperties(extensionProperties)
    new PipelineContext(parseExensionProperties)
  }
}
