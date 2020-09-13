package com.cosminsanda

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

case class Statistic(population_m: Double, updated_at_ts: String)
case class GeoDataRow(city: String, country: String, statistic: Statistic)

class ProcessorsTest extends AnyFunSuite with BeforeAndAfterAll {

    val spark: SparkSession = SparkSession
        .builder()
        .master("local[2]")
        .appName("Helios Testing")
        .config("spark.sql.streaming.schemaInference", "true")
        .getOrCreate()

    import spark.implicits._

    override def afterAll() {
        spark.sparkContext.stop()
    }

    test("the geo mapping statistics is working correctly") {
        Seq(
            GeoDataRow("COPENHAGEN", "DENMARK", Statistic(5.8, "2020-08-02T18:00:00")),
            GeoDataRow("LONDON", "UNITED KINGDOM", Statistic(66.5, "2020-08-02T18:00:00")),
            GeoDataRow("COPENHAGEN", "DENMARK", Statistic(5.7, "2020-08-03T18:00:00"))
        )
            .toDS()
            .createOrReplaceTempView("statistics")

        val processors = new Processors(spark)

        val geoMap = processors.getLatestGeoMapStatistics

        val copenhagenData = geoMap
            .filter("city_p == 'Copenhagen'")
            .select("population", "country")
            .collectAsList()

        assert(copenhagenData.size() == 1, "a city must be listed only once")
        assert(copenhagenData.get(0).getAs[Double]("population") == 5.7, "the population listed for a city must be the most up to date one.")
        assert(copenhagenData.get(0).getAs[String]("country") == "Denmark", "the country name must be transformed to look capitalized.")

        spark.sql("DROP TABLE `statistics`")
    }

}
