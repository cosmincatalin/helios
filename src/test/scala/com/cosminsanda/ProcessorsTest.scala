package com.cosminsanda

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

case class GeoDataRow(city: String, country: String, population_M: Double, updated_at_ts: String)

class ProcessorsTest extends AnyFunSuite {

    test("the geo mapping statistics is working correctly") {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[2]")
            .appName("Helios Testing")
            .config("spark.sql.streaming.schemaInference", "true")
            .getOrCreate()

        implicit val sqlContext: SQLContext = spark.sqlContext
        import spark.implicits._

        val statisticsData = MemoryStream[GeoDataRow]
        statisticsData.addData(
            GeoDataRow("COPENHAGEN", "DENMARK", 5.8, "2020-08-02T18:00:00"),
            GeoDataRow("LONDON", "UNITED KINGDOM", 66.5, "2020-08-02T18:00:00"),
            GeoDataRow("COPENHAGEN", "DENMARK", 5.7, "2020-08-03T18:00:00")
        )

        val processors = new Processors(spark)
        st

        val geoMap = processors.getLatestGeoMapStatistics

        val copenhagenData = geoMap
            .filter("city_p == 'Copenhagen'")
            .select("population", "country")
            .collectAsList()

        assert(copenhagenData.size() == 1, "a city must be listed only once")
        assert(copenhagenData.get(0).getAs[Double]("population") == 5.7, "the population listed for a city must be the most up to date one.")
        assert(copenhagenData.get(0).getAs[String]("country") == "Denmark", "the country name must be transformed to look capitalized.")

        statisticsData.addData(
            GeoDataRow("COPENHAGEN", "DENMARK", 5.5, "2020-08-02T18:00:00")
        )

        val newCopenhagenData = geoMap
            .filter("city_p == 'Copenhagen'")
            .select("population", "country")
            .collectAsList()

        assert(copenhagenData.size() == 1, "a city must be listed only once")
        assert(copenhagenData.get(0).getAs[Double]("population") == 5.7, "the population listed for a city must be the most up to date one.")
        assert(copenhagenData.get(0).getAs[String]("country") == "Denmark", "the country name must be transformed to look capitalized.")
    }

}
