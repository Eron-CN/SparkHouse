package com.hibbo.bankDemo.common

class ExplorationContext(private val extensionProperties: Map[String, String]) {
  def getExtensionProperties(): Map[String, String] = {
    extensionProperties
  }
}
