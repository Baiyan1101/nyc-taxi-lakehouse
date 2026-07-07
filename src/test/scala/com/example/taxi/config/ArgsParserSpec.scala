package com.example.taxi.config

import org.scalatest.funsuite.AnyFunSuite

class ArgsParserSpec extends AnyFunSuite {
  test("parses required ingestion arguments") {
    val parsed = ArgsParser.parseIngestionArgs(
      Array(
        "--format",
        "parquet",
        "--input",
        "data/raw/yellow_taxi/year=2024/month=01/",
        "--sample-size",
        "5"
      )
    )

    parsed match {
      case Right(config) =>
        assert(config.inputFormat == "parquet")
        assert(config.inputPaths == Seq("data/raw/yellow_taxi/year=2024/month=01/"))
        assert(config.sampleSize == 5)
      case Left(error) =>
        fail(error)
    }
  }

  test("requires at least one input path") {
    val parsed = ArgsParser.parseIngestionArgs(Array("--format", "parquet"))

    parsed match {
      case Left(error) => assert(error.contains("Missing required argument"))
      case Right(_) => fail("Expected parsing to fail")
    }
  }

  test("parses data quality output path") {
    val parsed = ArgsParser.parseDataQualityArgs(
      Array(
        "--format",
        "parquet",
        "--input",
        "data/raw/yellow_taxi/year=2024/month=01/",
        "--output",
        "data/reports/quality/yellow_taxi/year=2024/month=01"
      )
    )

    parsed match {
      case Right(config) =>
        assert(config.ingestionConfig.inputFormat == "parquet")
        assert(config.ingestionConfig.inputPaths == Seq("data/raw/yellow_taxi/year=2024/month=01/"))
        assert(config.outputPath.contains("data/reports/quality/yellow_taxi/year=2024/month=01"))
      case Left(error) =>
        fail(error)
    }
  }

  test("requires clean trips output path") {
    val parsed = ArgsParser.parseCleanTripsArgs(
      Array(
        "--format",
        "parquet",
        "--input",
        "data/raw/yellow_taxi/year=2024/month=01/"
      )
    )

    parsed match {
      case Left(error) => assert(error.contains("Missing required argument"))
      case Right(_) => fail("Expected parsing to fail")
    }
  }

  test("parses clean trips output path") {
    val parsed = ArgsParser.parseCleanTripsArgs(
      Array(
        "--format",
        "parquet",
        "--input",
        "data/raw/yellow_taxi/year=2024/month=01/",
        "--output",
        "data/cleaned/yellow_taxi",
        "--expected-start-date",
        "2024-01-01",
        "--expected-end-date",
        "2024-02-01"
      )
    )

    parsed match {
      case Right(config) =>
        assert(config.ingestionConfig.inputFormat == "parquet")
        assert(config.ingestionConfig.inputPaths == Seq("data/raw/yellow_taxi/year=2024/month=01/"))
        assert(config.outputPath == "data/cleaned/yellow_taxi")
        assert(config.partitionColumn == "pickup_date")
        assert(config.expectedStartDate.contains("2024-01-01"))
        assert(config.expectedEndDate.contains("2024-02-01"))
      case Left(error) =>
        fail(error)
    }
  }

  test("requires clean trips date window bounds together") {
    val parsed = ArgsParser.parseCleanTripsArgs(
      Array(
        "--format",
        "parquet",
        "--input",
        "data/raw/yellow_taxi/year=2024/month=01/",
        "--output",
        "data/cleaned/yellow_taxi",
        "--expected-start-date",
        "2024-01-01"
      )
    )

    parsed match {
      case Left(error) => assert(error.contains("must be provided together"))
      case Right(_) => fail("Expected parsing to fail")
    }
  }

  test("parses curated model arguments") {
    val parsed = ArgsParser.parseCuratedModelArgs(
      Array(
        "--cleaned-input",
        "data/cleaned/yellow_taxi",
        "--zone-lookup",
        "data/raw/taxi_zone_lookup/taxi_zone_lookup.csv",
        "--output",
        "data/curated"
      )
    )

    parsed match {
      case Right(config) =>
        assert(config.cleanedInputPath == "data/cleaned/yellow_taxi")
        assert(config.zoneLookupPath == "data/raw/taxi_zone_lookup/taxi_zone_lookup.csv")
        assert(config.outputPath == "data/curated")
      case Left(error) =>
        fail(error)
    }
  }
}
