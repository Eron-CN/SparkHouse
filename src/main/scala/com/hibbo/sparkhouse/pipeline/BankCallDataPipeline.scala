package com.hibbo.sparkhouse.pipeline

import com.hibbo.sparkhouse.common.{Constants, PipelineContext}
import com.hibbo.sparkhouse.common.datatype.BankCall
import com.hibbo.sparkhouse.common.utils.CommonUtil
import com.hibbo.sparkhouse.transform.BankCallTransformer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

class BankCallDataPipeline extends DataPipeline {
  override def read(spark: SparkSession, rawInputPath: String, fileFormat: String): DataFrame = {
    var dataFrame = spark.emptyDataFrame
    if(!CommonUtil.isNullOrEmpty(rawInputPath)){
      if(Constants.CSV.equals(fileFormat)){
        dataFrame = spark.read
          .schema(BankCall.getSchema)
          .option("sep", ";")
          .option("header", true)
          .csv(rawInputPath).toDF
      }
    }
    dataFrame
  }

  override def transform(spark: SparkSession,
                         dataFrame: DataFrame,
                         persistLevel: StorageLevel,
                         context: PipelineContext): DataFrame = {
    BankCallTransformer.transform(spark, dataFrame, persistLevel, context)
  }

  override def output(spark: SparkSession,
                      dataFrame: DataFrame,
                      format: String,
                      outputPath: String): Unit = {
    dataFrame.show()
  }

}
