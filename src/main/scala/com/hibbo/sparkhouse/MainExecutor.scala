package com.hibbo.sparkhouse

import com.hibbo.sparkhouse.common.Dataset
import com.hibbo.sparkhouse.common.utils.CommonUtil
import com.hibbo.sparkhouse.pipeline.BankCallDataPipeline

object MainExecutor {
  def main(args: Array[String]): Unit = {
    val argsMap = CommonUtil.ParseArgs(args)
    val dataset = Dataset.parse(argsMap("--Dataset".toLowerCase))

    dataset match {
      case Dataset.BankCall =>
        new BankCallDataPipeline().run(argsMap)
    }
  }
}
