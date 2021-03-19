package com.hibbo.bankDemo.datatype

import org.apache.spark.sql.types._

case class BankCall(age: Double,
                    job: String,
                    marital: String,
                    edu: String,
                    credit_default: String,
                    housing: String,
                    loan: String,
                    contact: String,
                    month: String,
                    day: String,
                    dur: Double,
                    campaign: Double,
                    pdays: Double,
                    prev: Double,
                    pout: String,
                    emp_var_rate: Double,
                    cons_price_idx: Double,
                    cons_conf_idx: Double,
                    euribor3m: Double,
                    nr_employed: Double,
                    deposit: String)

object BankCall {
  def getSchema: StructType = {
    StructType(
        StructField("age", DoubleType, true) ::
        StructField("job", StringType, true) ::
        StructField("marital", StringType, true) ::
        StructField("edu", StringType, true) ::
        StructField("credit_default", StringType, true) ::
        StructField("housing", StringType, true) ::
        StructField("loan", StringType, true) ::
        StructField("contact", StringType, true) ::
        StructField("month", StringType, true) ::
        StructField("day", StringType, true) ::
        StructField("dur", DoubleType, true) ::
        StructField("campaign", DoubleType, true) ::
        StructField("pdays", DoubleType, true) ::
        StructField("prev", DoubleType, true) ::
        StructField("pout", StringType, true) ::
        StructField("emp_var_rate", DoubleType, true) ::
        StructField("cons_price_idx", DoubleType, true) ::
        StructField("cons_conf_idx", DoubleType, true) ::
        StructField("euribor3m", DoubleType, true) ::
        StructField("nr_employed", DoubleType, true) ::
        StructField("deposit", StringType, true) ::
        Nil
    )
  }
}


