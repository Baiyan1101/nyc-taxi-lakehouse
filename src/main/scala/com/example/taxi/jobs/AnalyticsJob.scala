package com.example.taxi.jobs

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

final case class AnalyticsConfig(
    curatedInputPath: String,
    outputPath: String,
    topN: Int = 10
)

final case class AnalyticsSummary(
    dailyTripSummaryRows: Long,
    hourlyDemandSummaryRows: Long,
    topPickupZonesRows: Long,
    topDropoffZonesRows: Long,
    topRoutesRows: Long,
    topTipZonesRows: Long,
    outputPath: String
)

object AnalyticsJob {
  def run(spark: SparkSession, config: AnalyticsConfig): AnalyticsSummary = {
    val factTrips = spark.read.parquet(s"${config.curatedInputPath}/fact_trips").cache()
    val dimZone = spark.read.parquet(s"${config.curatedInputPath}/dim_zone").cache()

    val dailyTripSummary = buildDailyTripSummary(factTrips).cache()
    val hourlyDemandSummary = buildHourlyDemandSummary(factTrips).cache()
    val topPickupZones = buildTopPickupZones(factTrips, dimZone, config.topN).cache()
    val topDropoffZones = buildTopDropoffZones(factTrips, dimZone, config.topN).cache()
    val topRoutes = buildTopRoutes(factTrips, dimZone, config.topN).cache()
    val topTipZones = buildTopTipZones(factTrips, dimZone, config.topN).cache()

    writeParquet(dailyTripSummary, s"${config.outputPath}/daily_trip_summary")
    writeParquet(hourlyDemandSummary, s"${config.outputPath}/hourly_demand_summary")
    writeParquet(topPickupZones, s"${config.outputPath}/top_pickup_zones")
    writeParquet(topDropoffZones, s"${config.outputPath}/top_dropoff_zones")
    writeParquet(topRoutes, s"${config.outputPath}/top_routes")
    writeParquet(topTipZones, s"${config.outputPath}/top_tip_zones")

    val summary = AnalyticsSummary(
      dailyTripSummaryRows = dailyTripSummary.count(),
      hourlyDemandSummaryRows = hourlyDemandSummary.count(),
      topPickupZonesRows = topPickupZones.count(),
      topDropoffZonesRows = topDropoffZones.count(),
      topRoutesRows = topRoutes.count(),
      topTipZonesRows = topTipZones.count(),
      outputPath = config.outputPath
    )

    Seq(
      factTrips,
      dimZone,
      dailyTripSummary,
      hourlyDemandSummary,
      topPickupZones,
      topDropoffZones,
      topRoutes,
      topTipZones
    ).foreach(_.unpersist())

    printSummary(summary)
    summary
  }

  def buildDailyTripSummary(factTrips: DataFrame): DataFrame = {
    factTrips
      .groupBy("pickup_date")
      .agg(
        count(lit(1)).alias("trip_count"),
        round(sum("total_amount"), 2).alias("total_revenue"),
        round(avg("fare_amount"), 2).alias("avg_fare_amount"),
        round(avg("trip_distance"), 2).alias("avg_trip_distance"),
        round(avg("trip_duration_minutes"), 2).alias("avg_trip_duration_minutes")
      )
      .orderBy("pickup_date")
  }

  def buildHourlyDemandSummary(factTrips: DataFrame): DataFrame = {
    factTrips
      .groupBy("pickup_hour")
      .agg(count(lit(1)).alias("trip_count"))
      .orderBy("pickup_hour")
  }

  def buildTopPickupZones(factTrips: DataFrame, dimZone: DataFrame, topN: Int): DataFrame = {
    val zones = dimZone.alias("z")

    factTrips
      .alias("f")
      .join(zones, col("f.pickup_location_id") === col("z.location_id"), "left")
      .groupBy(
        col("f.pickup_location_id").alias("location_id"),
        col("z.borough"),
        col("z.zone")
      )
      .agg(
        count(lit(1)).alias("trip_count"),
        round(sum(col("f.total_amount")), 2).alias("total_revenue")
      )
      .orderBy(desc("trip_count"))
      .limit(topN)
  }

  def buildTopDropoffZones(factTrips: DataFrame, dimZone: DataFrame, topN: Int): DataFrame = {
    val zones = dimZone.alias("z")

    factTrips
      .alias("f")
      .join(zones, col("f.dropoff_location_id") === col("z.location_id"), "left")
      .groupBy(
        col("f.dropoff_location_id").alias("location_id"),
        col("z.borough"),
        col("z.zone")
      )
      .agg(
        count(lit(1)).alias("trip_count"),
        round(sum(col("f.total_amount")), 2).alias("total_revenue")
      )
      .orderBy(desc("trip_count"))
      .limit(topN)
  }

  def buildTopRoutes(factTrips: DataFrame, dimZone: DataFrame, topN: Int): DataFrame = {
    val pickupZones = dimZone.alias("pickup_zone")
    val dropoffZones = dimZone.alias("dropoff_zone")

    factTrips
      .alias("f")
      .join(pickupZones, col("f.pickup_location_id") === col("pickup_zone.location_id"), "left")
      .join(dropoffZones, col("f.dropoff_location_id") === col("dropoff_zone.location_id"), "left")
      .groupBy(
        col("f.pickup_location_id"),
        col("pickup_zone.borough").alias("pickup_borough"),
        col("pickup_zone.zone").alias("pickup_zone"),
        col("f.dropoff_location_id"),
        col("dropoff_zone.borough").alias("dropoff_borough"),
        col("dropoff_zone.zone").alias("dropoff_zone")
      )
      .agg(
        count(lit(1)).alias("trip_count"),
        round(sum(col("f.total_amount")), 2).alias("total_revenue"),
        round(avg(col("f.total_amount")), 2).alias("avg_total_amount")
      )
      .orderBy(desc("total_revenue"))
      .limit(topN)
  }

  def buildTopTipZones(factTrips: DataFrame, dimZone: DataFrame, topN: Int): DataFrame = {
    val zones = dimZone.alias("z")

    factTrips
      .alias("f")
      .join(zones, col("f.pickup_location_id") === col("z.location_id"), "left")
      .groupBy(
        col("f.pickup_location_id").alias("location_id"),
        col("z.borough"),
        col("z.zone")
      )
      .agg(
        count(lit(1)).alias("trip_count"),
        round(avg(col("f.tip_percentage")), 2).alias("avg_tip_percentage")
      )
      .orderBy(desc("avg_tip_percentage"))
      .limit(topN)
  }

  private def writeParquet(dataFrame: DataFrame, outputPath: String): Unit = {
    dataFrame.write.mode("overwrite").parquet(outputPath)
  }

  private def printSummary(summary: AnalyticsSummary): Unit = {
    println("Analytics summary")
    println(s"daily_trip_summary rows: ${summary.dailyTripSummaryRows}")
    println(s"hourly_demand_summary rows: ${summary.hourlyDemandSummaryRows}")
    println(s"top_pickup_zones rows: ${summary.topPickupZonesRows}")
    println(s"top_dropoff_zones rows: ${summary.topDropoffZonesRows}")
    println(s"top_routes rows: ${summary.topRoutesRows}")
    println(s"top_tip_zones rows: ${summary.topTipZonesRows}")
    println(s"Output path: ${summary.outputPath}")
  }
}

