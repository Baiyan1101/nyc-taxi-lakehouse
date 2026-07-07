package com.example.taxi.jobs

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

final case class DataQualityConfig(
    ingestionConfig: RawTripIngestionConfig,
    outputPath: Option[String] = None
)

final case class DataQualityMetric(
    metricName: String,
    metricValue: Long,
    description: String
)

final case class DataQualityReport(metrics: Seq[DataQualityMetric])

object DataQualityReportJob {
  val KeyFields: Seq[String] = Seq(
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "PULocationID",
    "DOLocationID",
    "total_amount"
  )

  private val MaxReasonablePassengerCount = 8L

  def run(spark: SparkSession, config: DataQualityConfig): DataQualityReport = {
    val trips = RawTripIngestionJob.read(spark, config.ingestionConfig)
    val report = buildReport(trips)

    printReport(report)
    config.outputPath.foreach(writeReport(spark, report, _))

    report
  }

  def buildReport(trips: DataFrame): DataQualityReport = {
    val totalRows = trips.count()
    val nullCounts = countNulls(trips, KeyFields)
    val duplicateRows = totalRows - trips.dropDuplicates().count()

    val metrics = Seq(
      DataQualityMetric("total_rows", totalRows, "Total number of raw trip records"),
      DataQualityMetric("duplicate_rows", duplicateRows, "Rows duplicated across all ingested columns"),
      DataQualityMetric(
        "invalid_trip_distance_rows",
        countWhere(trips, col("trip_distance") <= 0),
        "Rows where trip_distance is less than or equal to 0"
      ),
      DataQualityMetric(
        "negative_fare_amount_rows",
        countWhere(trips, col("fare_amount") < 0),
        "Rows where fare_amount is negative"
      ),
      DataQualityMetric(
        "pickup_after_dropoff_rows",
        countWhere(
          trips,
          col("tpep_pickup_datetime").isNotNull &&
            col("tpep_dropoff_datetime").isNotNull &&
            col("tpep_pickup_datetime") > col("tpep_dropoff_datetime")
        ),
        "Rows where pickup timestamp is later than dropoff timestamp"
      ),
      DataQualityMetric(
        "abnormal_passenger_count_rows",
        countWhere(
          trips,
          col("passenger_count").isNull ||
            col("passenger_count") <= 0 ||
            col("passenger_count") > MaxReasonablePassengerCount
        ),
        s"Rows where passenger_count is null, <= 0, or > $MaxReasonablePassengerCount"
      )
    ) ++ nullCounts.map { case (field, count) =>
      DataQualityMetric(s"null_count_$field", count, s"Rows where $field is null")
    }

    DataQualityReport(metrics)
  }

  private def countNulls(trips: DataFrame, fields: Seq[String]): Map[String, Long] = {
    val aggregations = fields.map { field =>
      sum(when(col(field).isNull, 1L).otherwise(0L)).cast("long").alias(field)
    }

    val row = trips.agg(aggregations.head, aggregations.tail: _*).first()

    fields.zipWithIndex.map { case (field, index) =>
      field -> row.getAs[Long](index)
    }.toMap
  }

  private def countWhere(trips: DataFrame, condition: org.apache.spark.sql.Column): Long =
    trips.filter(condition).count()

  private def printReport(report: DataQualityReport): Unit = {
    println("Data quality report")
    report.metrics.foreach { metric =>
      println(s"${metric.metricName}: ${metric.metricValue} (${metric.description})")
    }
  }

  private def writeReport(spark: SparkSession, report: DataQualityReport, outputPath: String): Unit = {
    import spark.implicits._

    report.metrics
      .toDF()
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(outputPath)
  }
}

