package com.availity.spark.provider

import com.availity.spark.util.ReadWriteUtil
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.BeforeAndAfterAll

import java.io.File
import java.nio.file.Paths

class ReadWriteUtilSpec extends AnyFunSuite with BeforeAndAfterAll with SharedTestSuite {


  test("readDF should load DataFrame successfully") {
    val baseDir = Paths.get("").toAbsolutePath.toString + "/target/scala-2.12"
    val inputDir = baseDir + "/input_data"

    // Define the schema explicitly
    val visitsSchema = StructType(Array(
      StructField("provider_id", StringType, true),
      StructField("visit_name", StringType, true),
      StructField("date_of_service", StringType, true)
    ))

    // Sample Test Data
    val visitsData = Seq(
      ("visit1", "provider1", "2023-12-01"),
      ("visit2", "provider1", "2023-12-05"),
      ("visit3", "provider2", "2024-01-10")
    )

    // Create the visits DataFrame with the explicit schema
    val visitsDF = spark.createDataFrame(
      spark.sparkContext.parallelize(visitsData.map(Row.fromTuple)),
      visitsSchema
    )

    // Create input_data/visits directory if it doesn't exist
    val visitsDir = new File(inputDir + "/visits")
    visitsDir.mkdirs()

    // Write temp input data to the input_data path
    visitsDF.coalesce(1).write.mode("overwrite").option("header", "true").csv(visitsDir.getAbsolutePath)

    val df = ReadWriteUtil.readDF("visits", spark)

    assert(df.isInstanceOf[DataFrame])
  }

  test("writeDF should write DataFrame without errors") {
    val testData = Seq(
      (12L, "provider1", 2L),
      (1L, "provider2", 1L)
    )
    // Define the schema explicitly
    val testSchema = StructType(Array(
      StructField("month", LongType, true),
      StructField("provider_id", StringType, true),
      StructField("total_visits", LongType, true)
    ))

    // Create the test DataFrame with the explicit schema
    val testDF = spark.createDataFrame(
      spark.sparkContext.parallelize(testData.map(Row.fromTuple)),
      testSchema
    )
    // Execute the writeDF method within a try-catch block to catch any exceptions
    var exceptionThrown = false
    try {
      ReadWriteUtil.writeDF("test_entity", testDF)
    } catch {
      case _: Exception => exceptionThrown = true
    }

    assert(!exceptionThrown)  // Assert that no exception was thrown
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
  }
}

