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
}
