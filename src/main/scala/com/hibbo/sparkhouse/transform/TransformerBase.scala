package com.hibbo.sparkhouse.transform

import com.hibbo.sparkhouse.common.PipelineContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

trait TransformerBase {
  def transform(sparkSession: SparkSession,
                dataFrame: DataFrame,
                persistLevel: StorageLevel,
                context: PipelineContext): DataFrame
}
