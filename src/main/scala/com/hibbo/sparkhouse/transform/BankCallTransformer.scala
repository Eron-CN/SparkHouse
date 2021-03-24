package com.hibbo.sparkhouse.transform

import com.hibbo.sparkhouse.common.PipelineContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

object BankCallTransformer extends TransformerBase {
  override def transform(spark: SparkSession,
                         dataFrame: DataFrame,
                         persistLevel: StorageLevel,
                         context: PipelineContext): DataFrame = {
    val step = context.getExtensionProperties().getOrElse("step", "")
    step match {
      case "PeopleCountGroupByMarital" =>   dataFrame.groupBy("marital").count()
      case "PeopleCountGroupByJob"     =>   dataFrame.groupBy("job").count()
      case "PeopleCountGroupByEdu"     =>   dataFrame.groupBy("edu").count()
    }
  }
}
