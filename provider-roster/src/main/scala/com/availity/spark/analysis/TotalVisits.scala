package com.availity.spark.analysis

import com.availity.spark.util.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.countDistinct

/**
 * TotalVisitsAnalysis: An object responsible for analyzing total visits per provider.
 *
 * This object provides a single function, `analyze`, which aggregates visit data to calculate the
 * total number of distinct visits for each provider. It takes a DataFrame containing provider and
 * visit information as input and returns a DataFrame with the analysis results.
 */
object TotalVisits extends Logging {

  /**
   * Analyzes the input DataFrame to calculate the total visits per provider.
   * This function groups the data by provider attributes (ID, name, specialty) and
   * then uses `countDistinct` to aggregate the unique visit IDs for each provider.
   *
   * @param data The input DataFrame containing provider and visit information.
   * @return A DataFrame
   */
  def analyse(data: DataFrame): DataFrame = {

    logger.info("Fetching total number of visits per provider!")

    // Group by provider attributes and count distinct visits
    val resultDF = data.groupBy("provider_id", "first_name", "middle_name", "last_name", "provider_specialty")
      .agg(countDistinct("visit_id").as("total_visits"))

    resultDF
  }
}
