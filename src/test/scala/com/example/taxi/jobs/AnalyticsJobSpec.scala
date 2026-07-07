package com.example.taxi.jobs

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.sql.{Date, Timestamp}

class AnalyticsJobSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("analytics-job-spec")
      .master("local[1]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
  }

  override protected def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  test("builds analytics aggregates from curated fact and dimension tables") {
    val factTrips = spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          trip("trip-1", "2024-01-01 10:00:00", "2024-01-01 10:10:00", "2024-01-01", 10, 1, 2, 1L, 2.0, 10.0, 2.0, 15.0, 1, 10.0, 7.5, 20.0),
          trip("trip-2", "2024-01-01 10:30:00", "2024-01-01 10:50:00", "2024-01-01", 10, 1, 3, 2L, 4.0, 20.0, 4.0, 30.0, 1, 20.0, 7.5, 20.0),
          trip("trip-3", "2024-01-02 11:00:00", "2024-01-02 11:05:00", "2024-01-02", 11, 2, 3, 1L, 1.0, 8.0, 1.0, 10.0, 2, 5.0, 10.0, 12.5)
        )
      ),
      factSchema
    )

    val dimZone = spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          Row(1, "Manhattan", "Zone A", "Yellow Zone"),
          Row(2, "Queens", "Zone B", "Boro Zone"),
          Row(3, "Brooklyn", "Zone C", "Boro Zone")
        )
      ),
      StructType(
        Seq(
          StructField("location_id", IntegerType, nullable = false),
          StructField("borough", StringType, nullable = true),
          StructField("zone", StringType, nullable = true),
          StructField("service_zone", StringType, nullable = true)
        )
      )
    )

    val daily = AnalyticsJob.buildDailyTripSummary(factTrips)
    val hourly = AnalyticsJob.buildHourlyDemandSummary(factTrips)
    val pickupZones = AnalyticsJob.buildTopPickupZones(factTrips, dimZone, topN = 2)
    val dropoffZones = AnalyticsJob.buildTopDropoffZones(factTrips, dimZone, topN = 2)
    val routes = AnalyticsJob.buildTopRoutes(factTrips, dimZone, topN = 2)
    val tipZones = AnalyticsJob.buildTopTipZones(factTrips, dimZone, topN = 2)

    val jan1 = daily.filter(col("pickup_date") === Date.valueOf("2024-01-01")).first()
    assert(jan1.getAs[Long]("trip_count") == 2L)
    assert(jan1.getAs[Double]("total_revenue") == 45.0d)
    assert(jan1.getAs[Double]("avg_fare_amount") == 15.0d)

    assert(hourly.filter(col("pickup_hour") === 10).first().getAs[Long]("trip_count") == 2L)
    assert(pickupZones.first().getAs[Int]("location_id") == 1)
    assert(dropoffZones.first().getAs[Int]("location_id") == 3)

    val topRoute = routes.first()
    assert(topRoute.getAs[Int]("pickup_location_id") == 1)
    assert(topRoute.getAs[Double]("total_revenue") == 30.0d)

    assert(tipZones.first().getAs[Int]("location_id") == 1)
    assert(tipZones.first().getAs[Double]("avg_tip_percentage") == 20.0d)
  }

  private def trip(
      tripId: String,
      pickupDatetime: String,
      dropoffDatetime: String,
      pickupDate: String,
      pickupHour: Int,
      pickupLocationId: Int,
      dropoffLocationId: Int,
      passengerCount: Long,
      tripDistance: Double,
      fareAmount: Double,
      tipAmount: Double,
      totalAmount: Double,
      paymentTypeId: Int,
      tripDurationMinutes: Double,
      totalAmountPerMile: Double,
      tipPercentage: Double
  ): Row =
    Row(
      tripId,
      Timestamp.valueOf(pickupDatetime),
      Timestamp.valueOf(dropoffDatetime),
      Date.valueOf(pickupDate),
      pickupHour,
      pickupLocationId,
      dropoffLocationId,
      passengerCount,
      tripDistance,
      fareAmount,
      tipAmount,
      totalAmount,
      paymentTypeId,
      tripDurationMinutes,
      totalAmountPerMile,
      tipPercentage
    )

  private val factSchema = StructType(
    Seq(
      StructField("trip_id", StringType, nullable = false),
      StructField("pickup_datetime", TimestampType, nullable = true),
      StructField("dropoff_datetime", TimestampType, nullable = true),
      StructField("pickup_date", DateType, nullable = true),
      StructField("pickup_hour", IntegerType, nullable = true),
      StructField("pickup_location_id", IntegerType, nullable = true),
      StructField("dropoff_location_id", IntegerType, nullable = true),
      StructField("passenger_count", LongType, nullable = true),
      StructField("trip_distance", DoubleType, nullable = true),
      StructField("fare_amount", DoubleType, nullable = true),
      StructField("tip_amount", DoubleType, nullable = true),
      StructField("total_amount", DoubleType, nullable = true),
      StructField("payment_type_id", IntegerType, nullable = true),
      StructField("trip_duration_minutes", DoubleType, nullable = true),
      StructField("total_amount_per_mile", DoubleType, nullable = true),
      StructField("tip_percentage", DoubleType, nullable = true)
    )
  )
}

