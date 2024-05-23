package com.availity.spark.util

import org.apache.log4j.{LogManager, Logger}

/**
 * Logging: Trait to provide a logger instance and potential logging utilities for classes.
 *
 * This trait offers a streamlined way to incorporate logging into other classes within your
 * Spark application. It leverages Log4j as the underlying logging framework.
 */
trait Logging {
  // Obtain a Logger instance named after the current class
  val logger: Logger = LogManager.getLogger(getClass)
}