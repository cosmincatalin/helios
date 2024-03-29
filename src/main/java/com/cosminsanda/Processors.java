package com.cosminsanda;

import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.IOException;
import java.io.StringReader;
import java.util.concurrent.TimeoutException;

@RequiredArgsConstructor
public class Processors {

    private final SparkSession spark;

    StreamingQuery backUpGeoMap(Dataset<Row> df) throws TimeoutException {
        return df
            .groupBy("city", "country")
            .agg(functions.expr("COLLECT_LIST(STRUCT(population_m, updated_at_ts)) AS statistics"))
            .selectExpr(
                "ARRAY_JOIN(TRANSFORM(SPLIT(country, ' '), x -> CONCAT(UPPER(SUBSTRING(x, 1, 1)), LOWER(SUBSTRING(x, 2)))), ' ') AS country",
                "ARRAY_JOIN(TRANSFORM(SPLIT(city, ' '), x -> CONCAT(UPPER(SUBSTRING(x, 1, 1)), LOWER(SUBSTRING(x, 2)))), ' ') AS city_p",
                "ELEMENT_AT(ARRAY_SORT(statistics, (left, right) -> IF(left.updated_at_ts > right.updated_at_ts, -1, 1)), 1).population_M AS population")
            .writeStream()
            .queryName("statistics")
            .format("memory")
            .outputMode(OutputMode.Complete())
            .start();
    }

    public Dataset<Row> aggregateTemperatures(Dataset<Row> readings) {
        return readings
            .filter("reading IS NOT NULL")
            .selectExpr("reading", "reading.timestamp AS watermark")
            .withWatermark("watermark", "3 days")
            .filter("reading.timestamp > DATE_SUB(NOW(), 3)")
            .select("reading")
            .groupBy("reading.city")
            .agg(functions.expr("COLLECT_LIST(STRUCT(reading.timestamp, reading.celsius, reading.fahrenheit)) AS readings"))
            .selectExpr("city", "ELEMENT_AT(ARRAY_SORT(readings, (left, right) -> IF(left.timestamp > right.timestamp, -1, 1)), 1) AS reading")
            .selectExpr("city", "DATE_FORMAT(reading.timestamp, 'yyyy-MM-dd') AS timestamp", "reading.celsius AS celsius", "reading.fahrenheit AS fahrenheit");
    }

    public static Row extractXml(String xml) throws ParserConfigurationException, SAXException {
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
        } catch (SAXException | IOException ignored) {
        }
        return null;
    }

    public Dataset<Row> prepareOutput(Dataset<Row> df) {
        return df
            .join(spark.sql("SELECT country, city_p, population FROM statistics"), functions.expr("city = city_p"), "left")
            .select("timestamp", "country", "city", "population", "celsius", "fahrenheit");
    }

}
