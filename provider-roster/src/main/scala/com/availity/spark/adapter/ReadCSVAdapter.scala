package com.availity.spark.adapter

import com.availity.spark.config.AppConfig
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * ReadCSVAdapter: A concrete implementation of the ReadAdapter trait for reading CSV files.
 *
 * This class handles the specific details of reading data from CSV files using Spark's DataFrameReader.
 * It provides the flexibility to read files with or without a predefined schema, and with custom delimiters.
 */
class ReadCSVAdapter extends ReadAdapter {

  /**
   * Reads a DataFrame from a CSV file.
   *
   * @param filePath   The path to the CSV file.
   * @param spark      The SparkSession to use for reading.
   * @param schema     An optional schema to apply to the DataFrame.
   * @param delimiter  An optional delimiter to use (defaults to the value from AppConfig).
   * @return           An Option containing the DataFrame if successful, or None if an error occurs.
   */
  override def read(filePath: String, spark: SparkSession,  schema: Option[StructType] = None, delimiter: Option[String] = None): Option[DataFrame] = {
    try {
      val reader = spark.read.option("header", "true").option("delimiter", delimiter.getOrElse(AppConfig.defaultDelimiter))

      val df = schema match {
        case Some(s) => reader.schema(s).csv(filePath)
        case None    => reader.csv(filePath)
      }

      Some(df)
    } catch {
      case e: Exception =>
        println(s"Failed to read CSV file: $filePath. Error: ${e.getMessage}")
        None
    }
  }
}
