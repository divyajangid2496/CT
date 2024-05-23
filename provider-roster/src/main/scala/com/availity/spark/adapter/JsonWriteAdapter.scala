package com.availity.spark.adapter

import com.availity.spark.util.ConfigFileReaderUtil
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
 * JsonWriteAdapter: Implementation of the WriteAdapter trait for writing DataFrames to JSON files.
 *
 * This class handles the specifics of saving Spark DataFrames in JSON format. It supports partitioning
 * the output data based on specified columns for better organization and performance.
 */
class JsonWriteAdapter extends WriteAdapter {

  /**
   * Writes a DataFrame to JSON files, optionally partitioned by specified columns.
   *
   * @param dataFrame   The DataFrame to write.
   * @param outputEntity The name of the entity to which the data belongs (used to get the output directory).
   * @param partitionBy An optional sequence of column names to partition the output by.
   */
  override def write(dataFrame: DataFrame, outputEntity: String, partitionBy: Option[Seq[String]] = None): Unit = {
    val writer = dataFrame.write.mode(SaveMode.Overwrite)
    val outputDir = ConfigFileReaderUtil.getOutputDir(outputEntity)

    partitionBy match {
      case Some(columns) => writer.partitionBy(columns:_*)
      case None => // Do nothing if partitionBy is not specified
    }
    writer.json(outputDir)
  }
}

// Companion object for easy access
object JsonWriteAdapter extends JsonWriteAdapter
