package com.hibbo.bankDemo.transform

import com.hibbo.bankDemo.common.ExplorationContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

trait TransformerBase {
  def transform(sparkSession: SparkSession,
                dataFrame: DataFrame,
                persistLevel: StorageLevel,
                context: ExplorationContext): DataFrame
}
