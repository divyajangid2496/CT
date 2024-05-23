package com.availity.spark.adapter

import com.availity.spark.config.AppConfig
import com.availity.spark.util.{ConfigFileReaderUtil, Logging}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * WriteAdapter: Defines the contract for writing DataFrames to different file types.
 * This trait acts as an interface in the Adapter Pattern for writing data. It provides
 * a single method, `write`, which any concrete adapter (e.g., `JsonWriteAdapter`) must implement.
 */
trait WriteAdapter {
  def write(dataFrame: DataFrame, outputDirPath: String, partitionBy: Option[Seq[String]] = None): Unit
}

/**
 * WriteFileTypeAdapter: Adapts writing of DataFrames to different file types (JSON, Parquet, etc.)
 * - Determines the output file type from configuration.
 * - Delegates the actual writing to the appropriate adapter (JsonWriteAdapter, etc.).
 * - Handles exceptions that may occur during the write process.
 */
class WriteFileTypeAdapter extends Logging {

  /**
   * Writes a DataFrame to a file using the specified file type and optional partitioning.
   *
   * @param entityDF    The DataFrame to write.
   * @param entity      The name of the entity (e.g., "visits", "providers") for the output file.
   * @param partitionBy Optional sequence of column names to partition the output by.
   */
  def writeFile(entityDF: DataFrame, entity: String, partitionBy: Option[Seq[String]] = None): Unit = {
    try {
      val fileType = AppConfig.outputFileType

      this.apply(fileType).write(entityDF, entity, partitionBy)
    } catch {
      case e: Exception =>
        println(s"Failed to write entity: $entity. Error: ${e.getMessage}")
    }
  }

  /**
   * Factory method to get the appropriate WriteAdapter based on the file type.
   *
   * @param fileType The type of file to write (e.g., "json", "parquet").
   * @return         The WriteAdapter implementation for the specified file type.
   * @throws IllegalArgumentException If the file type is not supported.
   */
  def apply(fileType: String): WriteAdapter = fileType match {
    case "json" => new JsonWriteAdapter
    // Add more cases for other file types (e.g., "parquet") as needed
    case _ => throw new IllegalArgumentException(s"Unsupported file type: $fileType")
  }
}
