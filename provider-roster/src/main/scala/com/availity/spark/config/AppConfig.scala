package com.availity.spark.config

/**
 * AppConfig: Centralized configuration settings for the Spark application.
 */
object AppConfig {
  val inputFileType: String = "csv"
  val outputFileType: String = "json"
  val inputDir: String = "/input_data"
  val outputDir: String = "/output_data"
  val defaultDelimiter : String = ","
}

