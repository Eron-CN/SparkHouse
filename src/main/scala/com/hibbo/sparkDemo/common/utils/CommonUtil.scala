package com.hibbo.sparkDemo.common.utils

import com.hibbo.sparkDemo.common.Constants
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods

import java.io.ByteArrayInputStream
import java.net.URI
import java.nio.charset.StandardCharsets
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.ZoneId
import java.time.temporal.ChronoField
import java.util.zip.GZIPInputStream
import java.util.{Base64, Calendar, Date, HashSet, TimeZone}


object CommonUtil {
  val dateFormatter     = new SimpleDateFormat("yyyyMMdd")
  val dateFormatter2    = new SimpleDateFormat("yyyy-MM-dd")
  val utcTimeFormatter  = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
  val utcTimeFormatter2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  val MaxObjectLength = 3*1024*1024
  val MaxAnnotationCount = 500
  val MaxAnnotationLength = 1024
  val AnnotationTooLongText = "!!!Annotation_Too_Long!!!"

  {
    dateFormatter.setTimeZone(TimeZone.getTimeZone("UTC"))
    dateFormatter2.setTimeZone(TimeZone.getTimeZone("UTC"))
    utcTimeFormatter.setTimeZone(TimeZone.getTimeZone("UTC"))
    utcTimeFormatter2.setTimeZone(TimeZone.getTimeZone("UTC"))
  }

  def getCurrentDateTimeStamp: Timestamp = {
    val today: java.util.Date = Calendar.getInstance.getTime
    val now: String           = utcTimeFormatter2.format(today)
    val re                    = java.sql.Timestamp.valueOf(now)
    re
  }

  def int2Date(int: Int): Date = {
    dateFormatter.parse(String.valueOf(int))
  }

  def string2Date(dateString: String): Date = {
    if (dateString == null || dateString.isEmpty) {
      return null
    }
    if (dateString.length == 8) {
      dateFormatter.parse(dateString)
    } else if (dateString.length == 10) {
      dateFormatter2.parse(dateString)
    } else {
      throw new IllegalArgumentException("Input Date format is not supported: " + dateString)
    }
  }

  def daysBetween(startDate: Date, endDate: Date): Long = {
    val startDateUTC = startDate.toInstant.atZone(ZoneId.of("UTC"))
    val endDateUTC   = endDate.toInstant.atZone(ZoneId.of("UTC"))
    endDateUTC.getLong(ChronoField.EPOCH_DAY) - startDateUTC.getLong(ChronoField.EPOCH_DAY)
  }

  def getYear(date: Date): String = {
    val utcDate = date.toInstant.atZone(ZoneId.of("UTC"))
    utcDate.getYear
      .toString
  }

  def getMonthOfYear(date: Date): String = {
    val utcDate     = date.toInstant.atZone(ZoneId.of("UTC"))
    val monthOfYear = utcDate.getMonthValue
    if (monthOfYear < 10)
      "0" + monthOfYear.toString
    else
      monthOfYear.toString
  }

  def getDayOfMonth(date: Date): String = {
    val utcDate    = date.toInstant.atZone(ZoneId.of("UTC"))
    val dayOfMonth = utcDate.getDayOfMonth
    if (dayOfMonth < 10)
      "0" + dayOfMonth.toString
    else
      dayOfMonth.toString
  }

  def date2Int(date: Date): Int = {
    Integer.parseInt(dateFormatter.format(date))
  }

  def unzip(base64String: String): String = {
    if (base64String == null || base64String.isEmpty) {
      return ""
    }
    try {
      val bytes       = Base64.getDecoder.decode(base64String)
      val inputStream = new GZIPInputStream(new ByteArrayInputStream(bytes))
      val output      = scala.io.Source.fromInputStream(inputStream).mkString
      output
    } catch {
      case e: Exception => throw e
    }
  }

  def bytesToHex(bytes: Array[Byte], sep: Option[String]): String = {
    sep match {
      case None => bytes.map("%02x".format(_)).mkString
      case _    => bytes.map("%02x".format(_)).mkString(sep.get)
    }
  }

  def isNullOrEmpty(s: String): Boolean = {
    Option(s).getOrElse("").isEmpty
  }

  def detectFileFormat(path: String): String = { detectFileFormat(path, true) }

  def detectFileFormat(path: String, isRecursive: Boolean): String = {
    var fileFormat   = Constants.JSON // default to Json
    var continueLoop = true
    val filePath     = new Path(path)
    val fs           = FileSystem.get(URI.create(path), new Configuration())
    val iterator     = fs.listFiles(filePath, isRecursive)
    while (iterator.hasNext && continueLoop) {
      val filename = iterator.next().getPath.toString
      if (filename.endsWith(".parquet")) {
        fileFormat = Constants.PARQUET
        continueLoop = false
      } else if (filename.endsWith(".json") || filename.endsWith(".gz")) {
        fileFormat = Constants.JSON
        continueLoop = false
      }
    }
    fileFormat
  }

  def getSubFoldersRecursively(path: String): List[String] = {
    var continueLoop = true
    val ret          = new HashSet[String]
    val filePath     = new Path(path)
    val fs           = FileSystem.get(URI.create(path), new Configuration())
    val iterator     = fs.listFiles(filePath, true)
    while (iterator.hasNext && continueLoop) {
      val folderPath = iterator.next().getPath
      if (folderPath.getName == filePath.getName && folderPath.depth() == filePath.depth()) { // the input is a file
        ret.add(path)
        continueLoop = false
      } else {
        val parentPath = folderPath.getParent
        ret.add(parentPath.toString)
      }
    }
    import scala.collection.JavaConversions._
    ret.toList
  }

  def extractJsonFeature(input: String, feature: String): String = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    if (isNullOrEmpty(input) || isNullOrEmpty(feature))
      return ""
    (JsonMethods.parse(input) \ feature).extractOrElse[String]("")
  }

  def getByteCount(str: String, charsetName: String = "UTF-8"): Int =
    Option(str).getOrElse("").getBytes(charsetName).length

  def stringToByteArray(value : String ): Array[Byte] = {
    if (value == null || value == "")
    {
      return Array.emptyByteArray;
    }
    value.getBytes(StandardCharsets.UTF_8)
  }

  def byteArrayToString(value :  Array[Byte]  ): String = {
    if (value == null || value.isEmpty)
    {
      return "";
    }
    new String(value, StandardCharsets.UTF_8);
  }

  def truncateStringBySize(value : String , lengthOfByte: Int ): String = {
    if (value == null || value == "" || lengthOfByte <= 0)
    {
      return value;
    }

    val byteCount = value.getBytes(StandardCharsets.UTF_8).length

    if(byteCount > lengthOfByte){
      new String(value.getBytes(StandardCharsets.UTF_8).take(lengthOfByte), StandardCharsets.UTF_8);
    }
    else{
      value
    }
  }

  def truncateByteArrayBySize(bArray : Array[Byte] , lengthOfByte: Int ): Array[Byte] = {
    if (bArray == null || lengthOfByte <= 0)
    {
      return Array.emptyByteArray;
    }
    if(bArray.length > lengthOfByte){
      bArray.take(lengthOfByte)
    }
    else{
      bArray
    }
  }


  /**
   * Parse ExtensionProperties parameters.
   * The input format is "ParameterAName->ParameterA,ParameterBName->ParameterB"
   *
   * @param extensionProperties String containing the extension properties
   * @return Map[String,String] with Key: Parameter Name, Value: Parameter Value
   */
  def parseExtensionProperties(extensionProperties: String): Map[String, String] = {
    if (!CommonUtil.isNullOrEmpty(extensionProperties)) {
      extensionProperties
        .split(",")
        .filter(_.trim != "")
        .map(p => {
          val split = p.split("->")
          if(split.length.equals(2)) {
            (split(0).trim, split(1).trim)
          } else if(split.length.equals(1)) {
            (split(0).trim, "")
          } else {
            ("", "")
          }
        }).toMap
    } else {
      null
    }
  }
}
