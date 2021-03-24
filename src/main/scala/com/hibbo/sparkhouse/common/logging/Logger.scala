package com.hibbo.sparkhouse.common.logging

import org.apache.log4j.Level

object Logger {

  private[common] val logger = org.apache.log4j.Logger.getLogger(getClass.getSimpleName)

  def log(message: String, level: Level): Unit ={
    level match {
      case Level.TRACE  => logger.trace(message)
      case Level.INFO   => logger.info(message)
      case Level.DEBUG  => logger.debug(message)
      case Level.WARN   => logger.warn(message)
      case Level.ERROR  => logger.error(message)
    }
  }
}
