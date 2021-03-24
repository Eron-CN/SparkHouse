package com.hibbo.sparkhouse.common

class PipelineContext(private val extensionProperties: Map[String, String]) {
  def getExtensionProperties(): Map[String, String] = {
    extensionProperties
  }
}
