package com.availity.spark.analysis

import com.availity.spark.util.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, countDistinct, month}


/**
 * TotalVisitsPerMonth: An object responsible for analyzing total visits per provider per month.
 *
 * This object provides a single function, `analyze`, which aggregates visit data to calculate the
 * total number of distinct visits for each provider in each month. It takes a DataFrame containing
 * provider and visit information as input and returns a DataFrame with the analysis results.
 */
object TotalVisitsPerMonth extends Logging {

  /**
   * Analyzes the input DataFrame to calculate the total visits per provider per month.
   * This function extracts the month from the `date_of_service` column, groups the data by
   * `provider_id` and `month`, and then uses `countDistinct` to aggregate the unique visit IDs
   * for each provider within each month.
   *
   * @param data The input DataFrame containing provider and visit information.
   *             It is assumed to have columns `provider_id`, `date_of_service`, and `visit_id`.
   * @return A DataFrame
   */
  def analyse(data: DataFrame): DataFrame = {

    logger.info("Fetching total visits per provider per month!")

    // Extract the month from the date_of_service column
    val dataWithMonth = data.withColumn("month", month(col("date_of_service")))

    // Group by provider_id, month, and count distinct visits
    val resultDF = dataWithMonth.withColumn("month", month(col("date_of_service")))
      .groupBy("provider_id", "month").agg(countDistinct("visit_id").as("total_visits"))

    resultDF
  }

}
