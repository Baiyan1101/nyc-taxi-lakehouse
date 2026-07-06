package com.example.taxi.config

import com.example.taxi.jobs.RawTripIngestionConfig

import scala.util.Try

object ArgsParser {
  def parseIngestionArgs(args: Array[String]): Either[String, RawTripIngestionConfig] = {
    def read(options: List[String], config: RawTripIngestionConfig): Either[String, RawTripIngestionConfig] =
      options match {
        case Nil =>
          Right(config)

        case "--input" :: value :: tail =>
          read(tail, config.copy(inputPaths = config.inputPaths :+ value))

        case "--format" :: value :: tail =>
          read(tail, config.copy(inputFormat = value.toLowerCase))

        case "--sample-size" :: value :: tail =>
          Try(value.toInt).toOption match {
            case Some(size) if size >= 0 => read(tail, config.copy(sampleSize = size))
            case _ => Left(s"Invalid --sample-size value '$value'. Use a non-negative integer.")
          }

        case unknown :: _ =>
          Left(s"Unknown or incomplete argument '$unknown'")
      }

    read(args.toList, RawTripIngestionConfig(inputPaths = Seq.empty))
      .flatMap { config =>
        if (config.inputPaths.nonEmpty) Right(config)
        else Left("Missing required argument: --input <path>")
      }
  }
}
