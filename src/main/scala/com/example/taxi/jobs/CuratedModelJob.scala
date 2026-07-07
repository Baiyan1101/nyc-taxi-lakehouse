package com.example.taxi.jobs

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

final case class CuratedModelConfig(
    cleanedInputPath: String,
    zoneLookupPath: String,
    outputPath: String
)

final case class CuratedModelSummary(
    factTripRows: Long,
    dimDateRows: Long,
    dimZoneRows: Long,
    dimPaymentTypeRows: Long,
    outputPath: String
)

object CuratedModelJob {
  private val ZoneLookupSchema = StructType(
    Seq(
      StructField("LocationID", IntegerType, nullable = false),
      StructField("Borough", StringType, nullable = true),
      StructField("Zone", StringType, nullable = true),
      StructField("service_zone", StringType, nullable = true)
    )
  )

  def run(spark: SparkSession, config: CuratedModelConfig): CuratedModelSummary = {
    val cleanedTrips = spark.read.parquet(config.cleanedInputPath).cache()
    val zoneLookup = readZoneLookup(spark, config.zoneLookupPath)

    val factTrips = buildFactTrips(cleanedTrips).cache()
    val dimDate = buildDimDate(cleanedTrips).cache()
    val dimZone = buildDimZone(zoneLookup).cache()
    val dimPaymentType = buildDimPaymentType(spark).cache()

    factTrips
      .write
      .mode("overwrite")
      .partitionBy("pickup_date")
      .parquet(s"${config.outputPath}/fact_trips")

    dimDate
      .write
      .mode("overwrite")
      .parquet(s"${config.outputPath}/dim_date")

    dimZone
      .write
      .mode("overwrite")
      .parquet(s"${config.outputPath}/dim_zone")

    dimPaymentType
      .write
      .mode("overwrite")
      .parquet(s"${config.outputPath}/dim_payment_type")

    val summary = CuratedModelSummary(
      factTripRows = factTrips.count(),
      dimDateRows = dimDate.count(),
      dimZoneRows = dimZone.count(),
      dimPaymentTypeRows = dimPaymentType.count(),
      outputPath = config.outputPath
    )

    Seq(cleanedTrips, factTrips, dimDate, dimZone, dimPaymentType).foreach(_.unpersist())

    printSummary(summary)
    summary
  }

  def readZoneLookup(spark: SparkSession, path: String): DataFrame =
    spark.read
      .option("header", "true")
      .option("mode", "FAILFAST")
      .schema(ZoneLookupSchema)
      .csv(path)

  def buildFactTrips(cleanedTrips: DataFrame): DataFrame = {
    cleanedTrips
      .withColumn(
        "trip_id",
        sha2(
          concat_ws(
            "||",
            col("VendorID").cast("string"),
            col("tpep_pickup_datetime").cast("string"),
            col("tpep_dropoff_datetime").cast("string"),
            col("PULocationID").cast("string"),
            col("DOLocationID").cast("string"),
            col("trip_distance").cast("string"),
            col("total_amount").cast("string")
          ),
          256
        )
      )
      .select(
        col("trip_id"),
        col("tpep_pickup_datetime").alias("pickup_datetime"),
        col("tpep_dropoff_datetime").alias("dropoff_datetime"),
        col("pickup_date"),
        col("pickup_hour"),
        col("PULocationID").alias("pickup_location_id"),
        col("DOLocationID").alias("dropoff_location_id"),
        col("passenger_count"),
        col("trip_distance"),
        col("fare_amount"),
        col("tip_amount"),
        col("total_amount"),
        col("payment_type").cast("int").alias("payment_type_id"),
        col("trip_duration_minutes"),
        col("total_amount_per_mile"),
        col("tip_percentage")
      )
  }

  def buildDimDate(cleanedTrips: DataFrame): DataFrame = {
    cleanedTrips
      .select(col("pickup_date").alias("date"))
      .distinct()
      .withColumn("year", year(col("date")))
      .withColumn("month", month(col("date")))
      .withColumn("day", dayofmonth(col("date")))
      .withColumn("day_of_week", date_format(col("date"), "E"))
      .withColumn("day_of_week_number", dayofweek(col("date")))
      .withColumn("is_weekend", col("day_of_week_number").isin(1, 7))
      .orderBy("date")
  }

  def buildDimZone(zoneLookup: DataFrame): DataFrame = {
    zoneLookup
      .select(
        col("LocationID").alias("location_id"),
        col("Borough").alias("borough"),
        col("Zone").alias("zone"),
        col("service_zone")
      )
      .dropDuplicates("location_id")
      .orderBy("location_id")
  }

  def buildDimPaymentType(spark: SparkSession): DataFrame = {
    import spark.implicits._

    Seq(
      (1, "Credit card"),
      (2, "Cash"),
      (3, "No charge"),
      (4, "Dispute"),
      (5, "Unknown"),
      (6, "Voided trip")
    ).toDF("payment_type_id", "payment_type_description")
  }

  private def printSummary(summary: CuratedModelSummary): Unit = {
    println("Curated model summary")
    println(s"fact_trips rows: ${summary.factTripRows}")
    println(s"dim_date rows: ${summary.dimDateRows}")
    println(s"dim_zone rows: ${summary.dimZoneRows}")
    println(s"dim_payment_type rows: ${summary.dimPaymentTypeRows}")
    println(s"Output path: ${summary.outputPath}")
  }
}
