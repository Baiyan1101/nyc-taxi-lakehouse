package com.example.taxi

import com.example.taxi.config.ArgsParser
import com.example.taxi.jobs.{CleanTripsJob, DataQualityReportJob, RawTripIngestionJob}
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    args.headOption match {
      case Some("ingest-raw") =>
        runIngestion(args.drop(1))

      case Some("quality-report") =>
        runQualityReport(args.drop(1))

      case Some("clean-trips") =>
        runCleanTrips(args.drop(1))

      case _ =>
        printUsage()
        runSmokeCheck()
    }
  }

  private def runIngestion(args: Array[String]): Unit = {
    ArgsParser.parseIngestionArgs(args) match {
      case Left(error) =>
        System.err.println(s"Argument error: $error")
        printUsage()
        sys.exit(1)

      case Right(config) =>
        withSpark("nyc-taxi-raw-ingestion") { spark =>
          RawTripIngestionJob.run(spark, config)
        }
    }
  }

  private def runQualityReport(args: Array[String]): Unit = {
    ArgsParser.parseDataQualityArgs(args) match {
      case Left(error) =>
        System.err.println(s"Argument error: $error")
        printUsage()
        sys.exit(1)

      case Right(config) =>
        withSpark("nyc-taxi-data-quality-report") { spark =>
          DataQualityReportJob.run(spark, config)
        }
    }
  }

  private def runCleanTrips(args: Array[String]): Unit = {
    ArgsParser.parseCleanTripsArgs(args) match {
      case Left(error) =>
        System.err.println(s"Argument error: $error")
        printUsage()
        sys.exit(1)

      case Right(config) =>
        withSpark("nyc-taxi-clean-trips") { spark =>
          CleanTripsJob.run(spark, config)
        }
    }
  }

  private def runSmokeCheck(): Unit =
    withSpark("nyc-taxi-lakehouse") { spark =>
      import spark.implicits._

      Seq(("hello", 1))
        .toDF("word", "count")
        .show(truncate = false)
    }

  private def withSpark(appName: String)(body: SparkSession => Unit): Unit = {
    val spark = SparkSession
      .builder()
      .appName(appName)
      .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try body(spark)
    finally spark.stop()
  }

  private def printUsage(): Unit = {
    println(
      """Usage:
        |  sbt "run ingest-raw --format parquet --input data/raw/yellow_taxi/year=2024/month=01/*.parquet"
        |  sbt "run ingest-raw --format csv --input data/raw/yellow_taxi/year=2024/month=01/*.csv"
        |  sbt "run quality-report --format parquet --input data/raw/yellow_taxi/year=2024/month=01/*.parquet --output data/reports/quality/yellow_taxi/year=2024/month=01"
        |  sbt "run clean-trips --format parquet --input data/raw/yellow_taxi/year=2024/month=01/*.parquet --output data/cleaned/yellow_taxi --expected-start-date 2024-01-01 --expected-end-date 2024-02-01"
        |
        |Options:
        |  --input <path>          Input file or directory. Can be provided multiple times.
        |  --format <csv|parquet>  Input format. Defaults to parquet.
        |  --sample-size <n>       Number of sample rows to print. Defaults to 10.
        |  --output <path>         Output directory for quality-report or clean-trips.
        |  --expected-start-date   Optional clean-trips inclusive pickup_date lower bound, yyyy-MM-dd.
        |  --expected-end-date     Optional clean-trips exclusive pickup_date upper bound, yyyy-MM-dd.
        |""".stripMargin
    )
  }
}
