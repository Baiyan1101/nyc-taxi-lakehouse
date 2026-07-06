package com.example.taxi.jobs

import com.example.taxi.models.TaxiSchemas
import org.apache.spark.sql.{DataFrame, SparkSession}

final case class RawTripIngestionConfig(
    inputPaths: Seq[String],
    inputFormat: String = "parquet",
    sampleSize: Int = 10
)

final case class RawTripIngestionSummary(
    rowCount: Long,
    columnCount: Int,
    columns: Seq[String]
)

object RawTripIngestionJob {
  private val SupportedFormats = Set("csv", "parquet")

  def read(spark: SparkSession, config: RawTripIngestionConfig): DataFrame = {
    require(config.inputPaths.nonEmpty, "At least one input path is required")
    require(
      SupportedFormats.contains(config.inputFormat.toLowerCase),
      s"Unsupported input format '${config.inputFormat}'. Supported formats: ${SupportedFormats.toSeq.sorted.mkString(", ")}"
    )

    config.inputFormat.toLowerCase match {
      case "csv" =>
        spark.read
          .option("header", "true")
          .option("mode", "FAILFAST")
          .schema(TaxiSchemas.yellowTaxiTripSchema)
          .csv(config.inputPaths: _*)

      case "parquet" =>
        spark.read
          .schema(TaxiSchemas.yellowTaxiTripSchema)
          .parquet(config.inputPaths: _*)
    }
  }

  def run(spark: SparkSession, config: RawTripIngestionConfig): RawTripIngestionSummary = {
    val trips = read(spark, config)
    val summary = RawTripIngestionSummary(
      rowCount = trips.count(),
      columnCount = trips.columns.length,
      columns = trips.columns.toSeq
    )

    printSummary(summary)
    trips.show(config.sampleSize, truncate = false)

    summary
  }

  private def printSummary(summary: RawTripIngestionSummary): Unit = {
    println("Raw trip ingestion summary")
    println(s"Rows: ${summary.rowCount}")
    println(s"Columns: ${summary.columnCount}")
    println(s"Column names: ${summary.columns.mkString(", ")}")
  }
}

