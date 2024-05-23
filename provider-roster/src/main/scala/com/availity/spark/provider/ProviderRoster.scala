package com.availity.spark.provider

import com.availity.spark.analysis.{TotalVisits, TotalVisitsPerMonth}
import com.availity.spark.util.{Logging, ReadWriteUtil}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions.{array, avg, broadcast, col, collect_list, count, countDistinct, lit, month}

/**
 * ProviderRoster: Spark application to analyze provider visits and their distribution over time.
 *  It is the Main entry point of the application and is responsible to
 *  Initiate Spark Session, read data, perform analysis, and
 *  write results to the output directory.
 */
object ProviderRoster extends Logging{

  /**
   * The main function that starts the Spark application.
   * @param args Command-line arguments (currently unused)
   */
  def main(args: Array[String]): Unit = {
    logger.info("Starting Spark App")

    // Initialize Spark Session
    val spark = SparkSession.builder().appName("ProviderRosterTest").master("local").getOrCreate()

    logger.info("Reading Input Data")
    // Read visits and providers data by invoking the helper function
    val dataDfs = readInputData(spark)
    val visits = dataDfs._1
    val providers = dataDfs._2

    // Invoke the helper function to perform analysis
    val analysedDataDFs = analyseVisits(visits, providers)
    val totalVisitsDF = analysedDataDFs._1
    val totalVisitsPerMonthDF = analysedDataDFs._2

    // Write the output using a helper function
    writeOutputData(totalVisitsDF, totalVisitsPerMonthDF)

    // Stopping the spark session at the end
    spark.stop()
  }

  /**
   * Helper function to read input data from files.
   *
   * @param spark The SparkSession to use for reading.
   * @return      Tuple containing DataFrames for visits and providers.
   */
  private def readInputData(spark: SparkSession) : (DataFrame, DataFrame) = {
    // Schema definition for visits entity
    val visits_schema : Option[StructType] = Some(StructType(Array(
      StructField("visit_id", StringType, true),
      StructField("provider_id", StringType, true),
      StructField("date_of_service", StringType, true)
    )))

    // Read visits and providers data into a DataFrames using ReadWriteUtil
    (ReadWriteUtil.readDF("visits", spark, schema = visits_schema),
      ReadWriteUtil.readDF("providers", spark, delimiter = Some("|")))
  }

  /**
   * Helper function to perform visit analysis.
   *
   * @param visits     DataFrame containing visit data.
   * @param providers  DataFrame containing provider data.
   * @return           Tuple containing DataFrames for total visits and total visits per month.
   */
  private def analyseVisits(visits: DataFrame, providers: DataFrame) : (DataFrame, DataFrame) = {
    logger.info("Joining data")
    // Join data on key "provider_id", using broadcast join to improve performance.
    val joinedData = broadcast(providers).join(visits, Seq("provider_id"), "inner")

    // Invoking the method which queries the data to fetch the total number of visits and Total visits per month
    (TotalVisits.analyse(joinedData),
      TotalVisitsPerMonth.analyse(joinedData))
  }

  /**
   * Helper function to write analysis results to the output directory.
   *
   * @param totalVisitsDF          DataFrame containing total visits per provider.
   * @param totalVisitsPerMonthDF  DataFrame containing total visits per provider per month.
   */
  private def writeOutputData(totalVisitsDF: DataFrame, totalVisitsPerMonthDF: DataFrame): Unit = {
    // Writing the result of the total number of visits per provider analysis to the output location
    totalVisitsDF.printSchema()
    ReadWriteUtil.writeDF("visits_per_provider", totalVisitsDF, Some(Seq("provider_specialty")))

    totalVisitsPerMonthDF.printSchema()
    // Writing the result of the total number of visits per provider per month analysis to the output location
    ReadWriteUtil.writeDF("visits_per_month", totalVisitsPerMonthDF, None)
  }

}
