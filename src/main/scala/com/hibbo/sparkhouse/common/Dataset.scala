package com.hibbo.sparkhouse.common

/**
 * Set of datasets supported by ODIN-ML.
 * Each new data set requires a new value here.
 * The string is the value of the parameter the client must pass in.
 * For example. To use the Reply Pair dataset we must pass "ReplyPair"
 *
 * This needs to be case insensitive so be careful to use tolower etc.
 */
object Dataset extends Enumeration {
  val BankCall = Value("BankCall")

  /**
   * Take a string and return the value if it corresponds to a dataset or None if it does not.
   * @param name the string to lookup in the Dataset Enum
   * @return the data set corresponding to the string or None.
   */
  def tryGetValue(name: String): Option[Value] = {
    values.find(_.toString.toLowerCase == name.toLowerCase())
  }

  /**
   * Returns the dataset value corresponding to the name argument or throws an illegal argument exception.
   * We want to remove this version as it is better to return an option and have the client decide what to do if the category is not found.
   * Removing this is a larger refactoring task.
   * @param name
   * @return
   */
  def parse(name: String): Value = {
    values.find(_.toString.toLowerCase == name.toLowerCase()).getOrElse(throw new IllegalArgumentException(s"Invalid dataset $name"))
  }

}
