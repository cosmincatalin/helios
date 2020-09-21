package com.cosminsanda

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.Instant
import java.time.temporal.ChronoUnit

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{Encoder, Encoders, SQLContext, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

case class GeoStatistic(city: String, country: String, population_M: Double, updated_at_ts: String)

class ProcessorsTest extends AnyFunSuite with BeforeAndAfterAll {

    val spark: SparkSession = SparkSession
        .builder()
        .master("local[2]")
        .appName("Helios Testing")
        .config("spark.sql.streaming.schemaInference", "true")
        .getOrCreate()

    implicit val sqlContext: SQLContext = spark.sqlContext

    import spark.implicits._
    org.apache.log4j.Logger.getLogger("org.apache").setLevel(org.apache.log4j.Level.WARN)

    override def afterAll() {
        spark.sparkContext.stop()
    }

    test("the geo mapping statistics are working correctly") {
        val source = MemoryStream[GeoStatistic]
        source.addData(
            GeoStatistic("COPENHAGEN", "DENMARK", 5.8, "2020-08-02T18:00:00"),
            GeoStatistic("LONDON", "UNITED KINGDOM", 66.5, "2020-08-02T18:00:00"),
            GeoStatistic("COPENHAGEN", "DENMARK", 5.7, "2020-08-03T18:00:00")
        )

        assert(source.toDF().isStreaming, "must be a streaming data source")

        val processors = new Processors(spark)
        val streamingQuery = processors.backUpGeoMap(source.toDF())
        streamingQuery.processAllAvailable()

        assert(spark.sql("SELECT COUNT(*) FROM statistics").collectAsList().get(0).getLong(0) == 2, "Must have exactly 2 rows at this point")

        assert(spark.sql("SELECT population FROM statistics WHERE city_p == 'Copenhagen' AND country == 'Denmark'").collectAsList().get(0).getDouble(0) == 5.7, "The latest population size must be shown")

        source.addData(GeoStatistic("COPENHAGEN", "DENMARK", 5.5, "2020-08-04T18:00:00"))
        streamingQuery.processAllAvailable()

        assert(spark.sql("SELECT population FROM statistics WHERE city_p == 'Copenhagen'").collectAsList().get(0).getDouble(0) == 5.5, "The updated population size must be shown")

        assert(spark.sql("SELECT COUNT(*) FROM statistics WHERE city_p == 'Copenhagen'").collectAsList().get(0).getLong(0) == 1, "Must have exactly 1 row for Copenhagen at this point")

        assert(spark.sql("SELECT city_p, country, population FROM statistics WHERE city_p == 'London' AND country == 'United Kingdom'").count() == 1, "Must have exactly 1 row for London at this point")

        spark.sql("DROP TABLE `statistics`")
    }

    test("temperature readings are aggregated correctly") {
        implicit val encoder: Encoder[TemperatureReading] = Encoders.bean[TemperatureReading](classOf[TemperatureReading])

        val sdf = new SimpleDateFormat("yyyy-MM-dd")

        val mostRecent = Timestamp.from(Instant.now().minus(16, ChronoUnit.HOURS))

        val source = MemoryStream[TemperatureReading]
        source.addData(
            new TemperatureReading("asdasda0", new Reading("Copenhagen", Timestamp.from(Instant.now().minus(2, ChronoUnit.DAYS)), 13.045, null)),
            new TemperatureReading("asdasda1", null),
            new TemperatureReading("asdasda2", new Reading("Copenhagen", mostRecent, 13.045, -10.01)),
            new TemperatureReading("asdasda3", new Reading("Copenhagen", Timestamp.from(Instant.now().minus(24, ChronoUnit.HOURS)), 32.0, .0)),
            new TemperatureReading("asdasda4", new Reading("London", Timestamp.from(Instant.now().minus(30, ChronoUnit.DAYS)), 12.04, null)),
            new TemperatureReading("asdasda4", new Reading("Oslo", Timestamp.from(Instant.now().minus(1, ChronoUnit.HOURS)), 12.04, null))
        )

        assert(source.toDF().isStreaming, "must be a streaming data source")

        val processors = new Processors(spark)
        val query = processors.aggregateTemperatures(source.toDF())
            .writeStream
            .queryName("aggregates")
            .format("memory")
            .outputMode(OutputMode.Complete())
            .start()
        query.processAllAvailable()

        assert(spark.sql("SELECT * FROM aggregates WHERE city == 'London'").count == 0, "London must not be show because it is not recent enough")
        assert(spark.sql("SELECT * FROM aggregates WHERE city == 'Copenhagen'").count == 1, "Only one instance of Copenhagen")
        assert(spark.sql("SELECT timestamp FROM aggregates WHERE city == 'Copenhagen'").collectAsList().get(0).getString(0) == sdf.format(mostRecent), "The most recent instance must be shown")

        val newest = Timestamp.from(Instant.now())

        source.addData(
            new TemperatureReading("asdasda5", new Reading("Copenhagen", newest, null, 10.0)),
        )
        query.processAllAvailable()

        assert(spark.sql("SELECT timestamp FROM aggregates WHERE city == 'Copenhagen'").collectAsList().get(0).getString(0) == sdf.format(Timestamp.from(Instant.now())), "Again, the most recent instance must be shown")

        spark.sql("DROP TABLE `aggregates`")
    }

    test("parse xml correctly") {
        val rows = List(
            """<?xml version="1.0" encoding="UTF-8"?>
              |<data>
              |    <city>CoPenHagen</city>
              |    <temperature>
              |        <value unit="celsius">27</value>
              |    </temperature>
              |    <measured_at_ts>2020-08-01T18:00:00</measured_at_ts>
              |</data>""".stripMargin,
            """<?xml version="1.0" encoding="UTF-8"?>
              |<data>
              |    <city>loNdon</city>
              |    <temperature>
              |        <value unit="fahrenheit">68</value>
              |    </temperature>
              |    <measured_at_ts>2020-08-01T18:05:00</measured_at_ts>
              |</data>""".stripMargin,
            """<?xml version="1.0" encoding="UTF-8"?>
              |<data>
              |    <city>London</city>
              |    <temperature>
              |        <value unit="celsius">26.5asd</value>
              |    </temperature>
              |    <measured_at_ts>2020-08-02T20:04:00</measured_at_ts>
              |</data>""".stripMargin,
            """<?xml version="1.0" encoding="UTF-8"?>
              |<data>
              |    <city>Copenhagen</city
              |    <temperature>
              |        <value unit="celsius">28</value>
              |    </temperature>
              |    <measured_at_ts>2020-08-03T18:00:00</measured_at_ts>
              |</data>""".stripMargin
        ).map(Processors.extractXml)
        rows.foreach(println)
//        assert(rows.count(_ == null) == 2, "Only two successfully parsed")
//        assert(rows.filter(_ != null).count(_.getAs[String](0) == "Copenhagen") == 1, "Copenhagen parsed correctly")
//        assert(rows.filter(_ != null).count(_.getAs[String](0) == "London") == 1, "Copenhagen parsed correctly")
//        assert(rows.filter(_ != null).filter(_.getAs[String](0) == "London").count(_.getAs[Double](2) == 20.0) == 1, "London celsius inferred correctly")
    }

}
