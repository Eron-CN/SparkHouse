package com.hibbo.sparkDemo.pipeline

import com.hibbo.sparkDemo.common.{Constants, PipelineContext}
import com.hibbo.sparkDemo.common.utils.CommonUtil
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

class ExploreBankCallDataPipeline extends DataPipeline {
  override def read(spark: SparkSession, rawInputPath: String, fileFormat: String): DataFrame = {
    if(!CommonUtil.isNullOrEmpty(rawInputPath)){
      fileFormat.toUpperCase match {
        case Constants.JSON => spark.read.json(rawInputPath).toDF
        case Constants.CSV  => spark.read.csv(rawInputPath).toDF
      }
    }else{
      spark.emptyDataFrame
    }
  }

  override def transform(sparkSession: SparkSession,
                         dataFrame: DataFrame,
                         persistLevel: StorageLevel,
                         context: PipelineContext): DataFrame = ???

  override def output(spark: SparkSession,
                      dataFrame: DataFrame,
                      format: String,
                      outputPath: String): Unit = {
    if(!CommonUtil.isNullOrEmpty(outputPath)){
      format.toUpperCase match {
        case Constants.JSON =>
          dataFrame.write
            .mode(SaveMode.Overwrite)
            .json(outputPath)
        case Constants.PARQUET =>
          dataFrame.write
            .mode(SaveMode.Overwrite)
            .parquet(outputPath)
      }
    }
  }

}
