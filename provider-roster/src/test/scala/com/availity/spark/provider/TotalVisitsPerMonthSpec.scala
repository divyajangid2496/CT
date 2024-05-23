package com.availity.spark.provider

import com.availity.spark.analysis.TotalVisitsPerMonth
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

/**
 * TotalVisitsPerMonthSpec: Unit tests for the TotalVisitsPerMonth analysis object.
 *
 * This test suite focuses on verifying the core functionality of the `analyse` function within
 * the `TotalVisitsPerMonth` object. It tests whether the function correctly calculates the total
 * number of visits per provider per month.
 */
class TotalVisitsPerMonthSpec extends AnyFunSuite with DataFrameComparer with BeforeAndAfterAll with SharedTestSuite {

  /**
   * Test case to verify the correct calculation of total visits per provider per month.
   */
  test("analyse should calculate total visits per provider per month correctly") {

    // Define the schema explicitly
    val providerSchema = StructType(Array(
      StructField("provider_id", StringType, true),
      StructField("first_name", StringType, true),
      StructField("last_name", StringType, true),
      StructField("middle_name", StringType, true),
      StructField("provider_specialty", StringType, true)
    ))

    // Define the schema explicitly
    val visitsSchema = StructType(Array(
      StructField("visit_id", StringType, true),
      StructField("provider_id", StringType, true),
      StructField("date_of_service", StringType, true)
    ))

    // Create test data for providers
    val testDataProviders = Seq(
      ("1","Bessie","B","Kuphal", "Cardiology"),
      ("2","Candice","C","Block", "Nephrology"),
      ("3","Margaret","B","Bartell", "Psychiatry")
    )

    // Create test data for visits
    val testDataVisits = Seq(
      ("1","1","2022-08-23"),
      ("2","1","2022-02-04"),
      ("3","1","2022-06-05"),
      ("4","1","2022-02-04"),
      ("5","1","2022-03-25"),
      ("6","2","2022-06-05"),
      ("7","2","2021-10-16"),
      ("8","2","2021-12-15"),
      ("9","2","2021-10-16"),
      ("10","3","2021-09-15"),
      ("11","3","2021-07-28"),
      ("12","3","2021-09-28")
    )

    // Create the visits DataFrame with the explicit schema
    val visitsDF = spark.createDataFrame(
      spark.sparkContext.parallelize(testDataVisits.map(Row.fromTuple)),
      visitsSchema
    )

    // Create the providers DataFrame with the explicit schema
    val providersDF = spark.createDataFrame(
      spark.sparkContext.parallelize(testDataProviders.map(Row.fromTuple)),
      providerSchema
    )

    // Join the data on `provider_id` to simulate input data for analysis
    val testJoinedData = providersDF.join(visitsDF, Seq("provider_id"), "inner")

    // Define the schema explicitly
    val expectedSchema = StructType(Array(
      StructField("provider_id", StringType, true),
      StructField("month", IntegerType, true),
      StructField("total_visits", LongType, false)
    ))

    // Define expected result DataFrame
    val expectedData = Seq(
      ("3", 9, 2L),
      ("3", 7, 1L),
      ("1", 8, 1L),
      ("1", 2, 2L),
      ("1", 6, 1L),
      ("1", 3, 1L),
      ("2", 6, 1L),
      ("2", 10, 2L),
      ("2", 12, 1L),
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData.map(Row.fromTuple)),
      expectedSchema
    )

    // Call the analyse function to get actual results
    val actualDF = TotalVisitsPerMonth.analyse(testJoinedData)

    // Assert that the actual and expected results match
    assert(actualDF.count() == expectedDF.count())  // Assuming we expect two unique members

    // Check specific values
    assertSmallDataFrameEquality(actualDF, expectedDF)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
  }
}
