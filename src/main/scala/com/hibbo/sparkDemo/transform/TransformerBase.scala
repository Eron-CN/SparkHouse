package com.hibbo.sparkDemo.transform

import com.hibbo.sparkDemo.common.PipelineContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

trait TransformerBase {
  def transform(sparkSession: SparkSession,
                dataFrame: DataFrame,
                persistLevel: StorageLevel,
                context: PipelineContext): DataFrame
}
