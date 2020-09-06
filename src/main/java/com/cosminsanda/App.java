package com.cosminsanda;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

public class App {

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        Config conf = ConfigFactory.load();
        Logger logger = LogManager.getLogger(App.class);

        org.apache.log4j.Logger.getLogger("org.apache").setLevel(org.apache.log4j.Level.WARN);

        SparkSession spark = SparkSession
            .builder()
            .appName("Helios")
            .master("local")
            .config("spark.sql.streaming.schemaInference", true)
            .getOrCreate();

        spark.udf().register("PARSE", (UDF1<String, Row>) xml -> {
                SAXParserFactory factory = SAXParserFactory.newInstance();
                SAXParser saxParser = factory.newSAXParser();

                ReadingHandler readingHandler = new ReadingHandler();
                try {
                    saxParser.parse(new InputSource(new StringReader(xml)), readingHandler);
                    Reading reading = readingHandler.getReading();
                    return RowFactory.create(
                        reading.getCity(),
                        reading.getTimestamp(),
                        reading.getCelsius(),
                        reading.getFahrenheit()
                    );
                } catch (SAXException ex) {
                    logger.trace(String.format("A reading had invalid format: %s", xml), ex);
                }
                return null;
            }, new StructType(new StructField[] {
                new StructField("city", StringType, false, Metadata.empty()),
                new StructField("timestamp", TimestampType, false, Metadata.empty()),
                new StructField("celsius", DoubleType, false, Metadata.empty()),
                new StructField("fahrenheit", DoubleType, false, Metadata.empty())
            }
        ));

        Dataset<Row> statistics = spark
            .readStream()
            .option("multiLine", true)
            .option("mode", "PERMISSIVE")
            .json(conf.getString("statistics.location"))
            .withColumn("updated_at_ts", functions.expr("TO_TIMESTAMP(updated_at_ts, 'yyyy-MM-dd‘T‘HH:mm:ss')"))
            .withColumnRenamed("city", "city_s");

        Dataset<Row> readings = spark
            .readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", conf.getString("kafka.bootstrap.servers"))
            .option("subscribe", conf.getString("kafka.topic"))
            .option("startingOffsets", "latest")
            .load()
            .selectExpr("CAST(value AS STRING) AS raw_xml")
            .withColumn("reading", functions.expr("PARSE(raw_xml)"));

        readings
            .writeStream()
            .queryName("Back up data")
            .format("parquet")
            .option("path", conf.getString("backup.location"))
            .option("checkpointLocation", conf.getString("spark.checkpoint.location"))
            .trigger(Trigger.ProcessingTime(Duration.create(1, TimeUnit.MINUTES)))
            .start();

        readings
            .select("reading")
            .filter("reading IS NOT NULL")
            .groupBy("reading.city")
            .agg(functions.expr("COLLECT_LIST(STRUCT(reading.timestamp, reading.celsius, reading.fahrenheit)) AS readings"))
            .selectExpr("city", "ELEMENT_AT(ARRAY_SORT(readings, (left, right) -> IF(left.timestamp > right.timestamp, -1, 1)), 1) AS reading")
            .selectExpr("city", "DATE_FORMAT(reading.timestamp, 'yyyy-MM-dd') AS timestamp", "reading.celsius AS celsius", "reading.fahrenheit AS fahrenheit")
            .writeStream()
            .queryName("Live temperature")
            .outputMode(OutputMode.Update())
            .format("console")
            .option("truncate", "false")
            .start();

        spark.streams().awaitAnyTermination();
    }
}
