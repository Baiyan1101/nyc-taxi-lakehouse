package com.example.taxi

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("nyc-taxi-lakehouse")
      .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    Seq(("hello", 1))
      .toDF("word", "count")
      .show(truncate = false)

    spark.stop()
  }
}

