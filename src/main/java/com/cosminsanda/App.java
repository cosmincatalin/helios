package com.cosminsanda;

import com.audienceproject.util.cli.Arguments;
import com.typesafe.config.ConfigFactory;
import lombok.extern.log4j.Log4j2;
import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.concurrent.duration.Duration;

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

        val processors = new Processors(spark);

        spark.udf().register("PARSE", (UDF1<String, Row>) Processors::extractXml, new StructType(new StructField[] {
                new StructField("city", StringType, false, Metadata.empty()),
                new StructField("timestamp", TimestampType, false, Metadata.empty()),
                new StructField("celsius", DoubleType, false, Metadata.empty()),
                new StructField("fahrenheit", DoubleType, false, Metadata.empty())
            }
        ));

        val geoData = spark
            .readStream()
            .format("json")
            .option("multiline", true)
            .load(conf.getString("geo.data.location"));

        val countriesStatistics = processors.getGeoMap(geoData);

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

        val consoleOutput = new Processors.ConsoleOutput(countriesStatistics);

        processors
            .aggregateTemperatures(readings)
            .writeStream()
            .queryName("Live temperature")
            .outputMode(OutputMode.Complete())
            .foreachBatch((Dataset<Row> batchDF, Long batchId) -> consoleOutput.prepareOutput(batchDF).show(false))
            .start();

        spark.streams().awaitAnyTermination();
    }
}
