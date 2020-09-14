package com.cosminsanda

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

case class GeoData(city: String, country: String, population_M: Double, updated_at_ts: String)

class ProcessorsTest extends AnyFunSuite with BeforeAndAfterAll {

    val spark: SparkSession = SparkSession
        .builder()
        .master("local[2]")
        .appName("Helios Testing")
        .config("spark.sql.streaming.schemaInference", "true")
        .getOrCreate()

    implicit val sqlContext: SQLContext = spark.sqlContext

    import spark.implicits._

    override def afterAll() {
        spark.sparkContext.stop()
    }

    test("the geo mapping statistics is working correctly") {
        val source = MemoryStream[GeoData]
        source.addData(
            GeoData("COPENHAGEN", "DENMARK", 5.8, "2020-08-02T18:00:00"),
            GeoData("LONDON", "UNITED KINGDOM", 66.5, "2020-08-02T18:00:00"),
            GeoData("COPENHAGEN", "DENMARK", 5.7, "2020-08-03T18:00:00")
        )

        assert(source.toDF().isStreaming, "must be a streaming data source")

        val processors = new Processors(spark)
        val streamingQuery = processors.backUpGeoMap(source.toDF())
        streamingQuery.processAllAvailable()

        assert(spark.sql("SELECT COUNT(*) FROM statistics").collectAsList().get(0).getLong(0) == 2, "Must have exactly 2 rows at this point")

        assert(spark.sql("SELECT population FROM statistics WHERE city_p == 'Copenhagen' AND country == 'Denmark'").collectAsList().get(0).getDouble(0) == 5.7, "The latest population size must be shown")

        source.addData(GeoData("COPENHAGEN", "DENMARK", 5.5, "2020-08-04T18:00:00"))
        streamingQuery.processAllAvailable()

        assert(spark.sql("SELECT population FROM statistics WHERE city_p == 'Copenhagen'").collectAsList().get(0).getDouble(0) == 5.5, "The updated population size must be shown")

        assert(spark.sql("SELECT COUNT(*) FROM statistics WHERE city_p == 'Copenhagen'").collectAsList().get(0).getLong(0) == 1, "Must have exactly 1 row for Copenhagen at this point")

        assert(spark.sql("SELECT city_p, country, population FROM statistics WHERE city_p == 'London' AND country == 'United Kingdom'").count() == 1, "Must have exactly 1 row for London at this point")

        spark.sql("DROP TABLE `statistics`")
    }

}
