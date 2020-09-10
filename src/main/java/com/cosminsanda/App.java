package com.cosminsanda;

import com.audienceproject.util.cli.Arguments;
import com.typesafe.config.ConfigFactory;
import lombok.Cleanup;
import lombok.extern.log4j.Log4j2;
import lombok.val;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import scala.concurrent.duration.Duration;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.StringReader;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.types.DataTypes.*;

@Log4j2
public class App {

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        val conf = ConfigFactory.load();
        val arguments = new Arguments(args);

        org.apache.log4j.Logger.getLogger("org.apache").setLevel(org.apache.log4j.Level.WARN);

        val sparkBuilder = SparkSession
            .builder()
            .appName("Helios")
            .config("spark.sql.streaming.schemaInference", true);

        SparkSession spark;
        if (arguments.isSet("local")) {
            spark = sparkBuilder.master("local[*]").getOrCreate();
        } else {
            spark = sparkBuilder.getOrCreate();
        }

        spark.udf().register("PARSE", (UDF1<String, Row>) xml -> {
                SAXParserFactory factory = SAXParserFactory.newInstance();
                SAXParser saxParser = factory.newSAXParser();

                ReadingHandler readingHandler = new ReadingHandler();
                try {
                    @Cleanup val sr = new StringReader(xml);
                    saxParser.parse(new InputSource(sr), readingHandler);
                    Reading reading = readingHandler.getReading();
                    return RowFactory.create(
                        reading.getCity(),
                        reading.getTimestamp(),
                        reading.getCelsius(),
                        reading.getFahrenheit()
                    );
                } catch (SAXException ignored) {
                }
                return null;
            }, new StructType(new StructField[] {
                new StructField("city", StringType, false, Metadata.empty()),
                new StructField("timestamp", TimestampType, false, Metadata.empty()),
                new StructField("celsius", DoubleType, false, Metadata.empty()),
                new StructField("fahrenheit", DoubleType, false, Metadata.empty())
            }
        ));

        spark
            .readStream()
            .format("json")
            .option("multiline", true)
            .load(conf.getString("geo.data.location"))
            .groupBy("city", "country")
            .agg(functions.expr("COLLECT_LIST(STRUCT(population_m, updated_at_ts)) AS statistics"))
            .selectExpr("country", "city", "ELEMENT_AT(ARRAY_SORT(statistics, (left, right) -> IF(left.updated_at_ts > right.updated_at_ts, -1, 1)), 1) AS statistic")
            .writeStream()
            .queryName("statistics")
            .format("memory")
            .outputMode(OutputMode.Complete())
            .start();

        val countriesStatistics = spark
            .sql("SELECT " +
                "ARRAY_JOIN(TRANSFORM(SPLIT(country, ' '), x -> CONCAT(UPPER(SUBSTRING(x, 1, 1)), LOWER(SUBSTRING(x, 2)))), ' ') AS country," +
                "ARRAY_JOIN(TRANSFORM(SPLIT(city, ' '), x -> CONCAT(UPPER(SUBSTRING(x, 1, 1)), LOWER(SUBSTRING(x, 2)))), ' ') AS city_p," +
                "FIRST(statistic.population_m) OVER (PARTITION BY city ORDER BY statistic.updated_at_ts DESC) AS population " +
                "FROM statistics")
            .groupBy("country", "city_p")
            .agg(functions.expr("FIRST(population) AS population"));

        log.info("Using Kafka bootstrap at " + conf.getString("kafka.bootstrap.servers"));

        val readings = spark
            .readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", conf.getString("kafka.bootstrap.servers"))
            .option("subscribe", conf.getString("kafka.topic"))
            .option("startingOffsets", "latest")
            .load()
            .selectExpr("CAST(value AS STRING) AS raw_xml")
            .withColumn("reading", functions.expr("PARSE(raw_xml)"));

        log.info("Backing up data at " + conf.getString("postgresql.url"));

        readings
            .selectExpr("raw_xml", "reading.city AS city", "reading.timestamp AS timestamp", "reading.celsius AS celsius", "reading.fahrenheit AS fahrenheit")
            .writeStream()
            .queryName("Back up data")
            .foreachBatch(PostgreSink::persist)
            .trigger(Trigger.ProcessingTime(Duration.create(1, TimeUnit.MINUTES)))
            .start();

        readings
            .filter("reading IS NOT NULL")
            .selectExpr("reading", "reading.timestamp AS watermark")
            .withWatermark("watermark", "3 days")
            .filter("reading.timestamp > DATE_SUB(NOW(), 3)")
            .selectExpr("reading")
            .groupBy("reading.city")
            .agg(functions.expr("COLLECT_LIST(STRUCT(reading.timestamp, reading.celsius, reading.fahrenheit)) AS readings"))
            .selectExpr("city", "ELEMENT_AT(ARRAY_SORT(readings, (left, right) -> IF(left.timestamp > right.timestamp, -1, 1)), 1) AS reading")
            .selectExpr("city", "DATE_FORMAT(reading.timestamp, 'yyyy-MM-dd') AS timestamp", "reading.celsius AS celsius", "reading.fahrenheit AS fahrenheit")
            .writeStream()
            .queryName("Live temperature")
            .outputMode(OutputMode.Complete())
            .foreachBatch((Dataset<Row> batchDF, Long batchId) -> batchDF
                .join(countriesStatistics, functions.expr("city = city_p"), "left")
                .select("timestamp", "country", "city", "population", "celsius", "fahrenheit")
                .show(false))
            .start();

        spark.streams().awaitAnyTermination();
    }
}
