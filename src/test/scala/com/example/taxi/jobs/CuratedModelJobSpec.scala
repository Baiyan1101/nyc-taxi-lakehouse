package com.example.taxi.jobs

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.sql.{Date, Timestamp}

class CuratedModelJobSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("curated-model-job-spec")
      .master("local[1]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
  }

  override protected def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  test("builds fact and dimension tables from cleaned trips") {
    val cleanedTrips = spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          Row(
            2,
            Timestamp.valueOf("2024-01-06 10:00:00"),
            Timestamp.valueOf("2024-01-06 10:15:00"),
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
            0.0d,
            15.0d,
            Date.valueOf("2024-01-06"),
            10,
            5.833333333333333d,
            25.0d
          )
        )
      ),
      StructType(
        Seq(
          StructField("VendorID", IntegerType, nullable = true),
          StructField("tpep_pickup_datetime", TimestampType, nullable = true),
          StructField("tpep_dropoff_datetime", TimestampType, nullable = true),
          StructField("passenger_count", LongType, nullable = true),
          StructField("trip_distance", DoubleType, nullable = true),
          StructField("RatecodeID", LongType, nullable = true),
          StructField("store_and_fwd_flag", StringType, nullable = true),
          StructField("PULocationID", IntegerType, nullable = true),
          StructField("DOLocationID", IntegerType, nullable = true),
          StructField("payment_type", LongType, nullable = true),
          StructField("fare_amount", DoubleType, nullable = true),
          StructField("extra", DoubleType, nullable = true),
          StructField("mta_tax", DoubleType, nullable = true),
          StructField("tip_amount", DoubleType, nullable = true),
          StructField("tolls_amount", DoubleType, nullable = true),
          StructField("improvement_surcharge", DoubleType, nullable = true),
          StructField("total_amount", DoubleType, nullable = true),
          StructField("congestion_surcharge", DoubleType, nullable = true),
          StructField("Airport_fee", DoubleType, nullable = true),
          StructField("trip_duration_minutes", DoubleType, nullable = true),
          StructField("pickup_date", DateType, nullable = true),
          StructField("pickup_hour", IntegerType, nullable = true),
          StructField("total_amount_per_mile", DoubleType, nullable = true),
          StructField("tip_percentage", DoubleType, nullable = true)
        )
      )
    )

    val zoneLookup = spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          Row(142, "Manhattan", "Lincoln Square East", "Yellow Zone"),
          Row(236, "Manhattan", "Upper East Side North", "Yellow Zone")
        )
      ),
      StructType(
        Seq(
          StructField("LocationID", IntegerType, nullable = false),
          StructField("Borough", StringType, nullable = true),
          StructField("Zone", StringType, nullable = true),
          StructField("service_zone", StringType, nullable = true)
        )
      )
    )

    val factTrips = CuratedModelJob.buildFactTrips(cleanedTrips)
    val dimDate = CuratedModelJob.buildDimDate(cleanedTrips)
    val dimZone = CuratedModelJob.buildDimZone(zoneLookup)
    val dimPaymentType = CuratedModelJob.buildDimPaymentType(spark)

    assert(factTrips.count() == 1L)
    assert(factTrips.columns.contains("trip_id"))
    assert(factTrips.select("pickup_location_id").first().getInt(0) == 142)
    assert(factTrips.select("payment_type_id").first().getInt(0) == 1)

    val dateRow = dimDate
      .select(
        col("date").cast("string").alias("date"),
        col("year"),
        col("month"),
        col("day"),
        col("is_weekend")
      )
      .first()

    assert(dateRow.getAs[String]("date") == "2024-01-06")
    assert(dateRow.getAs[Int]("year") == 2024)
    assert(dateRow.getAs[Int]("month") == 1)
    assert(dateRow.getAs[Int]("day") == 6)
    assert(dateRow.getAs[Boolean]("is_weekend"))

    assert(dimZone.count() == 2L)
    assert(dimZone.filter(col("location_id") === 142).select("borough").first().getString(0) == "Manhattan")

    assert(dimPaymentType.count() == 6L)
    assert(dimPaymentType.filter(col("payment_type_id") === 1).select("payment_type_description").first().getString(0) == "Credit card")
  }
}

