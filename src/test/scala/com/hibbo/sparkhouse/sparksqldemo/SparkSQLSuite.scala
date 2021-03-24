package com.hibbo.sparkhouse.sparksqldemo

import com.hibbo.sparkhouse.SparkFunSuite

class SparkSQLSuite extends SparkFunSuite{
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
}
