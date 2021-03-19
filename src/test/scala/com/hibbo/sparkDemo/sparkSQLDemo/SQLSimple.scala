package com.hibbo.sparkDemo.sparkSQLDemo

import com.hibbo.sparkDemo.SparkFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, get_json_object, row_number}

class SQLSimple extends SparkFunSuite{
  test("test spark sql"){
    val dfPeople = spark.read.json(getClass.getResource("/raw/demo/people.json").getPath)
    dfPeople.printSchema()
    dfPeople.show(false)
  }

  test("test join VS cogroup") {
    val list01 = List((1, "student_1101"), (2, "student_1201"), (3, "student_1301"), (1, "student_1102"), (2, "student_1202"))
    val list02 = List((1, "teacher01"), (2, "teacher02"), (3, "teacher03"))
//    spark.sparkContext.parallelize(list01)
//      .join(spark.sparkContext.parallelize(list02))
//      .foreach(println)

    spark.sparkContext.parallelize(list01)
      .cogroup(spark.sparkContext.parallelize(list02))
      .foreach(println)
  }

  test("test cogroup+flatmap => join"){
    val list01 = List((1, "student_1101"), (2, "student_1201"), (3, "student_1301"), (1, "student_1102"), (2, "student_1202"))
    val list02 = List((1, "teacher01"), (2, "teacher02"), (3, "teacher03"))

    val value = spark.sparkContext.parallelize(list01)
      .cogroup(spark.sparkContext.parallelize(list02))
    value.flatMap(item => item._2._1.flatMap(v => item._2._2.map(k => (item._1 ,(k, v))))).foreach(println)
//    spark.sparkContext.parallelize(list01).foreach(println)
  }

  test("test sql SELECT a,  COUNT(b)  FROM t WHERE c > 100 GROUP BY b"){

    val list = List(("math", "stu_a", 100), ("chinese", "stu_a", 95), ("english", "stu_a", 102),
      ("math", "stu_b", 105), ("chinese", "stu_b", 100), ("english", "stu_b", 112),
      ("math", "stu_c", 85), ("chinese", "stu_c", 106), ("english", "stu_c", 102))

      spark.sparkContext.parallelize(list)

  }

  test("test DF/DS"){
    val dataFrame = spark.read.json(getClass.getResource("/raw/demo/student.json").getPath)
//    dataFrame.groupBy("name","subject").avg("grade").show(24)
    // pivot: row to column
//    dataFrame.groupBy("name").pivot("subject").avg("grade").show()
    // window function
    val window = Window.partitionBy("name", "subject").orderBy("grade")
    dataFrame.select(
      col("name"),
      col("subject"),
      col("year"),
      col("grade"),
      row_number().over(window))
      .show()
  }


}
