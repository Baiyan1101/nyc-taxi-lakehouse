package com.example.taxi.jobs

import com.example.taxi.models.TaxiSchemas
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Timestamp

class CleanTripsJobSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("clean-trips-job-spec")
      .master("local[1]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
  }

  override protected def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  test("filters invalid rows and adds derived cleaning columns") {
    val validTrip = Row(
      2,
      Timestamp.valueOf("2024-01-01 10:00:00"),
      Timestamp.valueOf("2024-01-01 10:15:00"),
      1L,
      3.0d,
      1L,
      "N",
      142,
      236,
      1L,
      12.0d,
      1.0d,
      0.5d,
      3.0d,
      0.0d,
      1.0d,
      17.5d,
      2.5d,
      0.0d
    )

    val invalidDistance = Row(
      2,
      Timestamp.valueOf("2024-01-01 11:00:00"),
      Timestamp.valueOf("2024-01-01 11:10:00"),
      1L,
      0.0d,
      1L,
      "N",
      142,
      236,
      1L,
      10.0d,
      0.0d,
      0.5d,
      0.0d,
      0.0d,
      1.0d,
      11.5d,
      2.5d,
      0.0d
    )

    val negativeFare = Row(
      2,
      Timestamp.valueOf("2024-01-01 12:00:00"),
      Timestamp.valueOf("2024-01-01 12:10:00"),
      1L,
      2.0d,
      1L,
      "N",
      142,
      236,
      1L,
      -5.0d,
      0.0d,
      0.5d,
      0.0d,
      0.0d,
      1.0d,
      -3.5d,
      2.5d,
      0.0d
    )

    val badDuration = Row(
      2,
      Timestamp.valueOf("2024-01-01 13:30:00"),
      Timestamp.valueOf("2024-01-01 13:00:00"),
      1L,
      2.0d,
      1L,
      "N",
      142,
      236,
      1L,
      8.0d,
      0.0d,
      0.5d,
      0.0d,
      0.0d,
      1.0d,
      9.5d,
      2.5d,
      0.0d
    )

    val badPassengerCount = Row(
      2,
      Timestamp.valueOf("2024-01-01 14:00:00"),
      Timestamp.valueOf("2024-01-01 14:10:00"),
      0L,
      2.0d,
      1L,
      "N",
      142,
      236,
      1L,
      8.0d,
      0.0d,
      0.5d,
      0.0d,
      0.0d,
      1.0d,
      9.5d,
      2.5d,
      0.0d
    )

    val rawTrips = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(validTrip, invalidDistance, negativeFare, badDuration, badPassengerCount)),
      TaxiSchemas.yellowTaxiTripSchema
    )

    val cleaned = CleanTripsJob.clean(rawTrips)

    assert(cleaned.count() == 1L)

    val row = cleaned.select(
      col("pickup_date").cast("string").alias("pickup_date"),
      col("pickup_hour"),
      col("trip_duration_minutes"),
      col("total_amount_per_mile"),
      col("tip_percentage")
    ).first()

    assert(row.getAs[String]("pickup_date") == "2024-01-01")
    assert(row.getAs[Int]("pickup_hour") == 10)
    assert(row.getAs[Double]("trip_duration_minutes") == 15.0d)
    assert(math.abs(row.getAs[Double]("total_amount_per_mile") - 5.833333333333333d) < 0.000001d)
    assert(row.getAs[Double]("tip_percentage") == 25.0d)
  }

  test("filters pickup dates outside the expected source month") {
    val inMonthTrip = Row(
      2,
      Timestamp.valueOf("2024-01-31 23:00:00"),
      Timestamp.valueOf("2024-01-31 23:15:00"),
      1L,
      3.0d,
      1L,
      "N",
      142,
      236,
      1L,
      12.0d,
      1.0d,
      0.5d,
      3.0d,
      0.0d,
      1.0d,
      17.5d,
      2.5d,
      0.0d
    )

    val outOfMonthTrip = Row(
      2,
      Timestamp.valueOf("2009-01-01 10:00:00"),
      Timestamp.valueOf("2009-01-01 10:15:00"),
      1L,
      3.0d,
      1L,
      "N",
      142,
      236,
      1L,
      12.0d,
      1.0d,
      0.5d,
      3.0d,
      0.0d,
      1.0d,
      17.5d,
      2.5d,
      0.0d
    )

    val nextMonthTrip = Row(
      2,
      Timestamp.valueOf("2024-02-01 00:00:00"),
      Timestamp.valueOf("2024-02-01 00:10:00"),
      1L,
      2.0d,
      1L,
      "N",
      142,
      236,
      1L,
      8.0d,
      0.0d,
      0.5d,
      0.0d,
      0.0d,
      1.0d,
      9.5d,
      2.5d,
      0.0d
    )

    val rawTrips = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(inMonthTrip, outOfMonthTrip, nextMonthTrip)),
      TaxiSchemas.yellowTaxiTripSchema
    )

    val config = CleanTripsConfig(
      ingestionConfig = RawTripIngestionConfig(Seq.empty),
      outputPath = "",
      expectedStartDate = Some("2024-01-01"),
      expectedEndDate = Some("2024-02-01")
    )

    val cleaned = CleanTripsJob.clean(rawTrips, config)

    assert(cleaned.count() == 1L)
    assert(cleaned.select(col("pickup_date").cast("string")).first().getString(0) == "2024-01-31")
  }
}
