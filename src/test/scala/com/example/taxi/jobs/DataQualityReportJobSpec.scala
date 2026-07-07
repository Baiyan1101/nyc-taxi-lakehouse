package com.example.taxi.jobs

import com.example.taxi.models.TaxiSchemas
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Timestamp

class DataQualityReportJobSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("data-quality-report-job-spec")
      .master("local[1]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
  }

  override protected def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  test("builds quality metrics for common raw taxi data issues") {
    val validTrip = Row(
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

    val invalidTrip = Row(
      2,
      Timestamp.valueOf("2024-01-01 11:30:00"),
      Timestamp.valueOf("2024-01-01 11:00:00"),
      0L,
      -1.0d,
      1L,
      "N",
      null,
      236,
      1L,
      -5.0d,
      1.0d,
      0.5d,
      0.0d,
      0.0d,
      1.0d,
      -2.5d,
      2.5d,
      0.0d
    )

    val nullPassengerTrip = Row(
      1,
      null,
      Timestamp.valueOf("2024-01-01 12:00:00"),
      null,
      0.0d,
      1L,
      "N",
      142,
      236,
      2L,
      8.0d,
      0.5d,
      0.5d,
      0.0d,
      0.0d,
      1.0d,
      10.0d,
      2.5d,
      0.0d
    )

    val trips = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(validTrip, validTrip, invalidTrip, nullPassengerTrip)),
      TaxiSchemas.yellowTaxiTripSchema
    )

    val metrics = DataQualityReportJob
      .buildReport(trips)
      .metrics
      .map(metric => metric.metricName -> metric.metricValue)
      .toMap

    assert(metrics("total_rows") == 4L)
    assert(metrics("duplicate_rows") == 1L)
    assert(metrics("invalid_trip_distance_rows") == 2L)
    assert(metrics("negative_fare_amount_rows") == 1L)
    assert(metrics("pickup_after_dropoff_rows") == 1L)
    assert(metrics("abnormal_passenger_count_rows") == 2L)
    assert(metrics("null_count_tpep_pickup_datetime") == 1L)
    assert(metrics("null_count_passenger_count") == 1L)
    assert(metrics("null_count_PULocationID") == 1L)
  }
}

