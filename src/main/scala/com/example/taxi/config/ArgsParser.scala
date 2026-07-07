package com.example.taxi.config

import com.example.taxi.jobs.{AnalyticsConfig, CleanTripsConfig, CuratedModelConfig, DataQualityConfig, RawTripIngestionConfig}

import scala.util.Try

object ArgsParser {
  def parseIngestionArgs(args: Array[String]): Either[String, RawTripIngestionConfig] = {
    parseRawTripArgs(args, allowOutputPath = false).map { case (ingestionConfig, _, _, _) =>
      ingestionConfig
    }
  }

  def parseDataQualityArgs(args: Array[String]): Either[String, DataQualityConfig] = {
    parseRawTripArgs(args, allowOutputPath = true).map { case (ingestionConfig, outputPath, _, _) =>
      DataQualityConfig(ingestionConfig = ingestionConfig, outputPath = outputPath)
    }
  }

  def parseCleanTripsArgs(args: Array[String]): Either[String, CleanTripsConfig] = {
    parseRawTripArgs(args, allowOutputPath = true, allowDateWindow = true).flatMap {
      case (ingestionConfig, outputPath, expectedStartDate, expectedEndDate) =>
      outputPath match {
        case Some(path) =>
          (expectedStartDate, expectedEndDate) match {
            case (Some(_), None) | (None, Some(_)) =>
              Left("--expected-start-date and --expected-end-date must be provided together")
            case _ =>
              Right(
                CleanTripsConfig(
                  ingestionConfig = ingestionConfig,
                  outputPath = path,
                  expectedStartDate = expectedStartDate,
                  expectedEndDate = expectedEndDate
                )
              )
          }
        case None =>
          Left("Missing required argument for clean-trips: --output <path>")
      }
    }
  }

  def parseCuratedModelArgs(args: Array[String]): Either[String, CuratedModelConfig] = {
    def read(
        options: List[String],
        cleanedInputPath: Option[String],
        zoneLookupPath: Option[String],
        outputPath: Option[String]
    ): Either[String, CuratedModelConfig] =
      options match {
        case Nil =>
          (cleanedInputPath, zoneLookupPath, outputPath) match {
            case (Some(cleaned), Some(zoneLookup), Some(output)) =>
              Right(CuratedModelConfig(cleanedInputPath = cleaned, zoneLookupPath = zoneLookup, outputPath = output))
            case _ =>
              Left("Missing required arguments for build-curated: --cleaned-input <path> --zone-lookup <path> --output <path>")
          }

        case "--cleaned-input" :: value :: tail =>
          read(tail, Some(value), zoneLookupPath, outputPath)

        case "--zone-lookup" :: value :: tail =>
          read(tail, cleanedInputPath, Some(value), outputPath)

        case "--output" :: value :: tail =>
          read(tail, cleanedInputPath, zoneLookupPath, Some(value))

        case unknown :: _ =>
          Left(s"Unknown or incomplete argument '$unknown'")
      }

    read(args.toList, cleanedInputPath = None, zoneLookupPath = None, outputPath = None)
  }

  def parseAnalyticsArgs(args: Array[String]): Either[String, AnalyticsConfig] = {
    def read(
        options: List[String],
        curatedInputPath: Option[String],
        outputPath: Option[String],
        topN: Int
    ): Either[String, AnalyticsConfig] =
      options match {
        case Nil =>
          (curatedInputPath, outputPath) match {
            case (Some(curated), Some(output)) =>
              Right(AnalyticsConfig(curatedInputPath = curated, outputPath = output, topN = topN))
            case _ =>
              Left("Missing required arguments for build-analytics: --curated-input <path> --output <path>")
          }

        case "--curated-input" :: value :: tail =>
          read(tail, Some(value), outputPath, topN)

        case "--output" :: value :: tail =>
          read(tail, curatedInputPath, Some(value), topN)

        case "--top-n" :: value :: tail =>
          Try(value.toInt).toOption match {
            case Some(n) if n > 0 => read(tail, curatedInputPath, outputPath, n)
            case _ => Left(s"Invalid --top-n value '$value'. Use a positive integer.")
          }

        case unknown :: _ =>
          Left(s"Unknown or incomplete argument '$unknown'")
      }

    read(args.toList, curatedInputPath = None, outputPath = None, topN = 10)
  }

  private def parseRawTripArgs(
      args: Array[String],
      allowOutputPath: Boolean,
      allowDateWindow: Boolean = false
  ): Either[String, (RawTripIngestionConfig, Option[String], Option[String], Option[String])] = {
    def read(
        options: List[String],
        config: RawTripIngestionConfig,
        outputPath: Option[String],
        expectedStartDate: Option[String],
        expectedEndDate: Option[String]
    ): Either[String, (RawTripIngestionConfig, Option[String], Option[String], Option[String])] =
      options match {
        case Nil =>
          Right((config, outputPath, expectedStartDate, expectedEndDate))

        case "--input" :: value :: tail =>
          read(tail, config.copy(inputPaths = config.inputPaths :+ value), outputPath, expectedStartDate, expectedEndDate)

        case "--format" :: value :: tail =>
          read(tail, config.copy(inputFormat = value.toLowerCase), outputPath, expectedStartDate, expectedEndDate)

        case "--sample-size" :: value :: tail =>
          Try(value.toInt).toOption match {
            case Some(size) if size >= 0 =>
              read(tail, config.copy(sampleSize = size), outputPath, expectedStartDate, expectedEndDate)
            case _ => Left(s"Invalid --sample-size value '$value'. Use a non-negative integer.")
          }

        case "--output" :: value :: tail if allowOutputPath =>
          read(tail, config, Some(value), expectedStartDate, expectedEndDate)

        case "--output" :: _ if !allowOutputPath =>
          Left("--output is only supported by quality-report")

        case "--expected-start-date" :: value :: tail if allowDateWindow =>
          read(tail, config, outputPath, Some(value), expectedEndDate)

        case "--expected-end-date" :: value :: tail if allowDateWindow =>
          read(tail, config, outputPath, expectedStartDate, Some(value))

        case "--expected-start-date" :: _ | "--expected-end-date" :: _ =>
          Left("--expected-start-date and --expected-end-date are only supported by clean-trips")

        case unknown :: _ =>
          Left(s"Unknown or incomplete argument '$unknown'")
      }

    read(args.toList, RawTripIngestionConfig(inputPaths = Seq.empty), None, None, None)
      .flatMap { case (config, outputPath, expectedStartDate, expectedEndDate) =>
        if (config.inputPaths.nonEmpty) Right((config, outputPath, expectedStartDate, expectedEndDate))
        else Left("Missing required argument: --input <path>")
      }
  }
}
