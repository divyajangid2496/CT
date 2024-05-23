package com.availity.spark.adapter


import com.availity.spark.config.AppConfig
import com.availity.spark.util.{ConfigFileReaderUtil, Logging}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
 * ReadAdapter: Defines the contract for reading DataFrames from different file types.
 * This trait serves as the interface for the Adapter Pattern in the context of data reading. It defines
 * a single method, `read`, which any concrete adapter (e.g., `ReadCSVAdapter`) must implement.
 */
trait ReadAdapter {
  def read(filePath: String, spark: SparkSession, schema: Option[StructType], delimiter: Option[String]): Option[DataFrame]
}

/**
 * ReadFileTypeAdapter: Adapts reading of different file types (CSV, Parquet, etc.)
 * - Determines the file type from configuration.
 * - Retrieves the correct file path based on entity name.
 * - Delegates the actual reading to the appropriate adapter (ReadCSVAdapter, etc.) using the Adapter Pattern.
 * - Provides error handling for failed read operations.
 */
class ReadFileTypeAdapter(spark: SparkSession) extends Logging {

  /**
   * Reads a DataFrame from the specified entity's file using the appropriate adapter.
   *
   * @param entity     The name of the entity (e.g., "visits", "providers") corresponding to the file.
   * @param schema     Optional schema for the DataFrame (useful for CSV files).
   * @param delimiter  Optional delimiter for CSV files (default is ",").
   * @return           An Option containing the DataFrame if successful, None otherwise.
   */
  def readFile(entity: String, schema: Option[StructType], delimiter: Option[String]): Option[DataFrame] = {
    try {
      val fileType = AppConfig.inputFileType
      val filePath = ConfigFileReaderUtil.getInputDir(entity)

      this.apply(fileType).read(filePath, spark, schema, delimiter)
    } catch {
      case e: Exception =>
        println(s"Failed to read entity: $entity. Error: ${e.getMessage}")
        None
    }
  }

  /**
   * Factory method to get the appropriate ReadAdapter based on the file type.
   *
   * @param fileType The type of file to read (e.g., "csv").
   * @return The ReadAdapter implementation for the specified file type.
   * @throws IllegalArgumentException If the file type is not supported.
   */
  def apply(fileType: String): ReadAdapter = fileType match {
    case "csv" => new ReadCSVAdapter
    // Add more cases for other file types (e.g., "parquet") as needed
    case _ => throw new IllegalArgumentException(s"Unsupported file type: $fileType")
  }
}