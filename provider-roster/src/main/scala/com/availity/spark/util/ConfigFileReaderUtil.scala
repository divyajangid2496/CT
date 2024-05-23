package com.availity.spark.util

import com.availity.spark.config.AppConfig

/**
 * ConfigFileReaderUtil: Utility object to handle file-related configuration tasks.
 *
 * This object provides methods for:
 * - Determining the file type from a file name (e.g., "csv", "json").
 * - Constructing the full input file path for a given entity.
 * - Constructing the full output file path for a given entity.
 */
object ConfigFileReaderUtil extends Logging {

  /**
   * Extracts the file type (extension) from a file name.
   *
   * @param fileName  The name of the file (e.g., "data.csv").
   * @return          The file type (e.g., "csv") in lowercase.
   */
  def getFileType(fileType: String): String = {
    // Extract file extension to determine file type
    logger.info("Fetching file type from configs")
    val extension = fileType.split('.').lastOption.getOrElse("")
    extension.toLowerCase
  }

  /**
   * Constructs the full input file path for a given entity.
   * It combines the project's base directory with the `inputDir` from `AppConfig`
   * and the entity name to create the path.
   *
   * @param entity The name of the entity (e.g., "visits", "providers").
   * @return       The full input file path (e.g., "/project/input_data/visits/").
   */
  def getInputDir(entity: String): String = {
    // Get the base directory of the project (where the JAR is running from)
    val baseDir = new java.io.File(getClass.getProtectionDomain.getCodeSource.getLocation.getPath).getParent
    logger.info(s"Input Base dir: $baseDir")

    // Construct the full input file path
    val inputFilePath = baseDir + AppConfig.inputDir + "/" + entity + "/"

    inputFilePath
  }

  /**
   * Constructs the full output file path for a given entity.
   * It combines the project's base directory with the `outputDir` from `AppConfig`
   * and the entity name to create the path.
   *
   * @param entity The name of the entity (e.g., "visits", "providers").
   * @return       The full output file path (e.g., "/project/output_data/visits/").
   */
  def getOutputDir(entity: String): String = {
    // Get the base directory of the project
    val baseDir = new java.io.File(getClass.getProtectionDomain.getCodeSource.getLocation.getPath).getParent
    logger.info(s"Output Base dir: $baseDir" + "/" + AppConfig.outputDir + "/" + entity)

    // Construct the full output file path
    val outputFilePath = baseDir + AppConfig.outputDir + "/" + entity

    outputFilePath
  }
}

