package com.hibbo.sparkhouse.common.logging

import org.apache.log4j.Level

object LogLevel extends Enumeration {
  val trace = Level.TRACE
  val info  = Level.INFO
  val debug = Level.DEBUG
  val warn  = Level.WARN
  val error = Level.ERROR
}
