package com.cosminsanda;

import lombok.val;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.streaming.MemoryStream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import scala.Option;

public class DataOperationsTest {

    static SparkSession spark;

    @BeforeAll
    static void setup() {
        spark = SparkSession
            .builder()
            .master("local[2]")
            .appName("Helios Testing")
            .config("spark.sql.streaming.schemaInference", true)
            .getOrCreate();
    }

    @Test
    void createStatisticsTable(){
        val statisticsData = new MemoryStream<>(0, spark.sqlContext(), Option.empty(), Encoders.STRING());
    }
}
