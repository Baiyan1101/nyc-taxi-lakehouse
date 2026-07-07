package com.example.taxi.config

import com.example.taxi.jobs.{DataQualityConfig, RawTripIngestionConfig}

import scala.util.Try

object ArgsParser {
  def parseIngestionArgs(args: Array[String]): Either[String, RawTripIngestionConfig] = {
    parseRawTripArgs(args, allowOutputPath = false).map(_._1)
  }

  def parseDataQualityArgs(args: Array[String]): Either[String, DataQualityConfig] = {
    parseRawTripArgs(args, allowOutputPath = true).map { case (ingestionConfig, outputPath) =>
      DataQualityConfig(ingestionConfig = ingestionConfig, outputPath = outputPath)
    }
  }

  private def parseRawTripArgs(
      args: Array[String],
      allowOutputPath: Boolean
  ): Either[String, (RawTripIngestionConfig, Option[String])] = {
    def read(
        options: List[String],
        config: RawTripIngestionConfig,
        outputPath: Option[String]
    ): Either[String, (RawTripIngestionConfig, Option[String])] =
      options match {
        case Nil =>
          Right(config -> outputPath)

        case "--input" :: value :: tail =>
          read(tail, config.copy(inputPaths = config.inputPaths :+ value), outputPath)

        case "--format" :: value :: tail =>
          read(tail, config.copy(inputFormat = value.toLowerCase), outputPath)

        case "--sample-size" :: value :: tail =>
          Try(value.toInt).toOption match {
            case Some(size) if size >= 0 => read(tail, config.copy(sampleSize = size), outputPath)
            case _ => Left(s"Invalid --sample-size value '$value'. Use a non-negative integer.")
          }

        case "--output" :: value :: tail if allowOutputPath =>
          read(tail, config, Some(value))

        case "--output" :: _ if !allowOutputPath =>
          Left("--output is only supported by quality-report")

        case unknown :: _ =>
          Left(s"Unknown or incomplete argument '$unknown'")
      }

    read(args.toList, RawTripIngestionConfig(inputPaths = Seq.empty), None)
      .flatMap { case (config, outputPath) =>
        if (config.inputPaths.nonEmpty) Right(config -> outputPath)
        else Left("Missing required argument: --input <path>")
      }
  }
}
