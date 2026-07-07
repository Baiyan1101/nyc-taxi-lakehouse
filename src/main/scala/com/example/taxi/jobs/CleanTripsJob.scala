package com.example.taxi.jobs

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

final case class CleanTripsConfig(
    ingestionConfig: RawTripIngestionConfig,
    outputPath: String,
    partitionColumn: String = "pickup_date",
    expectedStartDate: Option[String] = None,
    expectedEndDate: Option[String] = None
)

final case class CleanTripsSummary(
    inputRows: Long,
    outputRows: Long,
    removedRows: Long,
    outputPath: String,
    partitionColumn: String
)

object CleanTripsJob {
  private val MaxReasonablePassengerCount = 8L
  private val MaxTripDurationMinutes = 6 * 60

  def run(spark: SparkSession, config: CleanTripsConfig): CleanTripsSummary = {
    val rawTrips = RawTripIngestionJob.read(spark, config.ingestionConfig)
    val inputRows = rawTrips.count()

    val cleanedTrips = clean(rawTrips, config).cache()
    val outputRows = cleanedTrips.count()

    cleanedTrips
      .write
      .mode("overwrite")
      .partitionBy(config.partitionColumn)
      .parquet(config.outputPath)

    cleanedTrips.unpersist()

    val summary = CleanTripsSummary(
      inputRows = inputRows,
      outputRows = outputRows,
      removedRows = inputRows - outputRows,
      outputPath = config.outputPath,
      partitionColumn = config.partitionColumn
    )

    printSummary(summary)
    summary
  }

  def clean(trips: DataFrame, config: CleanTripsConfig = CleanTripsConfig(RawTripIngestionConfig(Seq.empty), "")): DataFrame = {
    val durationMinutes =
      (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 60.0

    val cleaned = trips
      .filter(col("tpep_pickup_datetime").isNotNull)
      .filter(col("tpep_dropoff_datetime").isNotNull)
      .filter(col("trip_distance") > 0)
      .filter(col("fare_amount") >= 0)
      .filter(col("passenger_count").isNotNull)
      .filter(col("passenger_count") > 0 && col("passenger_count") <= MaxReasonablePassengerCount)
      .withColumn("trip_duration_minutes", durationMinutes)
      .filter(col("trip_duration_minutes") > 0)
      .filter(col("trip_duration_minutes") <= MaxTripDurationMinutes)
      .withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))
      .withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
      .withColumn("total_amount_per_mile", col("total_amount") / col("trip_distance"))
      .withColumn(
        "tip_percentage",
        when(col("fare_amount") > 0, col("tip_amount") / col("fare_amount") * 100.0).otherwise(lit(0.0))
      )

    applyExpectedDateWindow(cleaned, config)
  }

  private def applyExpectedDateWindow(trips: DataFrame, config: CleanTripsConfig): DataFrame = {
    (config.expectedStartDate, config.expectedEndDate) match {
      case (Some(startDate), Some(endDate)) =>
        trips
          .filter(col("pickup_date") >= lit(startDate))
          .filter(col("pickup_date") < lit(endDate))
      case (None, None) =>
        trips
      case _ =>
        throw new IllegalArgumentException(
          "expectedStartDate and expectedEndDate must be provided together"
        )
    }
  }

  private def printSummary(summary: CleanTripsSummary): Unit = {
    println("Clean trips summary")
    println(s"Input rows: ${summary.inputRows}")
    println(s"Output rows: ${summary.outputRows}")
    println(s"Removed rows: ${summary.removedRows}")
    println(s"Output path: ${summary.outputPath}")
    println(s"Partition column: ${summary.partitionColumn}")
  }
}
