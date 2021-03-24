package com.hibbo.sparkhouse.pipeline

import com.hibbo.sparkhouse.SparkFunSuite

class BankCallDataPipelineSuite extends SparkFunSuite{
  test("bank call data pipeline test"){
    val argsMap = getArgsMap()

    new BankCallDataPipeline().curate(spark, argsMap)
  }

  def getInputFilePath(): String = {
    getClass
      .getResource("/raw/bank/bank-additional-full.csv")
      .getPath
  }

  private def getArgsMap(): Map[String, String] = {
    Map[String, String](
      "--FilePath".toLowerCase -> getInputFilePath(),
      "--PipelineName" -> "BankCallData",
      "--InputFileFormat".toLowerCase -> "CSV",
      "--OutputFileFormat".toLowerCase -> "PARQUET",
      "--PersistLevel".toLowerCase -> "MEMORY_AND_DISK",
      "--OutputPath".toLowerCase -> "",
      "--ExtensionProperties".toLowerCase -> "step -> PeopleCountGroupByMarital"
    )
  }

}
