package com.availity.spark.provider

import com.availity.spark.analysis.TotalVisits
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * TotalVisitsSpec: Unit tests for the TotalVisits analysis object.
 *
 * This test suite focuses on verifying the correctness of the `analyse` function within the
 * `TotalVisits` object. It tests whether the function accurately calculates the total number
 * of visits for each provider.
 */
class TotalVisitsSpec extends AnyFunSuite with DataFrameComparer with BeforeAndAfterAll with SharedTestSuite {

  /**
   * Test case to verify the correct calculation of total visits per provider.
   */
  test("analyse should calculate total visits per provider correctly") {

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
      ("10","3","2021-12-15"),
      ("11","3","2021-10-28"),
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
      StructField("first_name", StringType, true),
      StructField("middle_name", StringType, true),
      StructField("last_name", StringType, true),
      StructField("provider_specialty", StringType, true),
      StructField("total_visits", LongType, false)
    ))

    // Define expected result DataFrame
    val expectedData = Seq(
      ("3","Margaret","Bartell","B", "Psychiatry", 3L),
      ("1","Bessie","Kuphal","B", "Cardiology", 5L),
      ("2","Candice","Block","C", "Nephrology", 4L)
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData.map(Row.fromTuple)),
      expectedSchema
    )

    // Call the analyse function to get actual results
    val actualDF = TotalVisits.analyse(testJoinedData)

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
