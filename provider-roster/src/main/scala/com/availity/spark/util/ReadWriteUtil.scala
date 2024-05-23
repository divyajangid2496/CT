package com.availity.spark.util

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.availity.spark.adapter.ReadFileTypeAdapter
import com.availity.spark.adapter.WriteFileTypeAdapter
import org.apache.spark.sql.types.StructType

/**
 * ReadWriteUtil: Utility object to streamline data reading and writing in Spark applications.
 *
 * Leverages Adapter Pattern:
 *  - Uses `ReadFileTypeAdapter` and `WriteFileTypeAdapter` to handle different file formats.
 *  - This design makes it easy to add support for new file types without modifying the core logic of this utility.
 */
object ReadWriteUtil extends Logging {

  /**
   * Reads a DataFrame from a file using the appropriate adapter based on the file type.
   *
   * @param entityName  The name of the entity (e.g., "visits", "providers") corresponding to the file.
   * @param spark       The SparkSession used for reading.
   * @param schema      Optional schema for the DataFrame (useful for CSV files).
   * @param delimiter   Optional delimiter for CSV files (default is ",").
   * @return            The loaded DataFrame.
   */
  def readDF(entityName: String, spark: SparkSession, schema: Option[StructType] = None, delimiter: Option[String] = None) : DataFrame = {
    val reader = new ReadFileTypeAdapter(spark)

    logger.info(s"Loading $entityName data")
    val dataDF = reader.readFile(entityName, schema, delimiter)

    dataDF.get
  }

  /**
   * Writes a DataFrame to a file using the appropriate adapter based on the file type.
   *
   * @param entityName  The name of the entity (e.g., "visits", "providers") corresponding to the output file.
   * @param entityDF    The DataFrame to write.
   * @param partitionBy Optional sequence of column names to partition the output by.
   */
  def writeDF(entityName: String, entityDF: DataFrame, partitionBy: Option[Seq[String]] = None) : Unit = {
    val writer = new WriteFileTypeAdapter()

    logger.info(s"Writing $entityName data to Output Directory! ")
    writer.writeFile(entityDF, entityName, partitionBy)
  }
}
