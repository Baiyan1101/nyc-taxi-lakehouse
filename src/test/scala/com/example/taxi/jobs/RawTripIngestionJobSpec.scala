package com.example.taxi.jobs

import com.example.taxi.models.TaxiSchemas
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.Files
import java.sql.Timestamp

class RawTripIngestionJobSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("raw-trip-ingestion-job-spec")
      .master("local[1]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
  }

  override protected def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  test("reads parquet trips with the declared yellow taxi schema") {
    val outputDir = Files.createTempDirectory("yellow-trip-parquet").toFile

    val rows = spark.sparkContext.parallelize(
      Seq(
        Row(
          2,
          Timestamp.valueOf("2024-01-01 10:00:00"),
          Timestamp.valueOf("2024-01-01 10:15:00"),
          1L,
          3.2d,
          1L,
          "N",
          142,
          236,
          1L,
          15.5d,
          1.0d,
          0.5d,
          3.0d,
          0.0d,
          1.0d,
          21.0d,
          2.5d,
          0.0d
        )
      )
    )

    spark
      .createDataFrame(rows, TaxiSchemas.yellowTaxiTripSchema)
      .write
      .mode("overwrite")
      .parquet(outputDir.getAbsolutePath)

    val trips = RawTripIngestionJob.read(
      spark,
      RawTripIngestionConfig(inputPaths = Seq(outputDir.getAbsolutePath), inputFormat = "parquet")
    )

    assert(trips.count() == 1)
    assert(trips.columns.toSeq == TaxiSchemas.yellowTaxiTripSchema.fieldNames.toSeq)
  }
}

