package com.cosminsanda;

import com.typesafe.config.ConfigFactory;
import lombok.Cleanup;
import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;

public class PostgreSink {

    public static void persist(Dataset<Row> batchDF, Long batchId) throws ClassNotFoundException, SQLException {
        val conf = ConfigFactory.load();

        Class.forName("org.postgresql.Driver");
        @Cleanup val connection = DriverManager.getConnection(conf.getString("postgresql.url"), conf.getString("postgresql.user"), conf.getString("postgresql.pwd"));
        connection.setAutoCommit(false);

        val statementInsert = connection.prepareStatement("INSERT INTO readings(id, raw_xml, city, timestamp, celsius, fahrenheit, active_from, active_to, active_flag) VALUES (MD5(?), ?, ?, ?, ?, ?, ?, ?, ?)");
        val statementUpdate = connection.prepareStatement("UPDATE readings SET active_to = ?  WHERE city = ? AND timestamp = (SELECT timestamp FROM readings WHERE city = ? AND timestamp < ? ORDER BY timestamp DESC LIMIT 1)");
        val statementFlagTrue = connection.prepareStatement("UPDATE readings SET active_flag = TRUE WHERE city = ? AND id = (SELECT id FROM readings WHERE city = ? ORDER BY timestamp DESC LIMIT 1)");
        val statementFlagFalse = connection.prepareStatement("UPDATE readings SET active_flag = FALSE  WHERE city = ? AND active_flag = TRUE");
        val statementReconcile = connection.prepareStatement("UPDATE readings AS r1 SET active_to = (SELECT active_from FROM readings AS r2 WHERE r2.city = r1.city AND r2.active_from > r1.active_from ORDER BY r2.active_from LIMIT 1) WHERE active_from IS NOT NULL AND city = ?");

        val cities = new ArrayList<String>();

        for (Row row: batchDF.collectAsList()) {
            statementInsert.setString(1, row.getAs("raw_xml"));
            statementInsert.setString(2, row.getAs("raw_xml"));
            statementInsert.setString(3, row.isNullAt(1) ? null : row.getAs("city"));
            statementInsert.setTimestamp(4, row.isNullAt(2) ? null : row.getAs("timestamp"));
            statementInsert.setObject(5, row.isNullAt(3) ? null : row.getAs ("celsius"), Types.DOUBLE);
            statementInsert.setObject(6, row.isNullAt(4) ? null : row.getAs ("fahrenheit"), Types.DOUBLE);
            statementInsert.setTimestamp(7, row.isNullAt(2) ? null : row.getAs("timestamp"));
            statementInsert.setTimestamp(8, null);
            statementInsert.setObject(9, row.isNullAt(1) ? null : false, Types.BOOLEAN);

            statementInsert.addBatch();

            if (!row.isNullAt(1)) {
                statementUpdate.setTimestamp(1, row.getAs("timestamp"));
                statementUpdate.setString(2, row.getAs("city"));
                statementUpdate.setString(3, row.getAs("city"));
                statementUpdate.setTimestamp(4, row.getAs("timestamp"));

                statementUpdate.addBatch();

                cities.add(row.getAs("city"));
            }
        }

        for (val city : cities) {
            statementFlagFalse.setString(1, city);
            statementFlagFalse.addBatch();

            statementFlagTrue.setString(1, city);
            statementFlagTrue.setString(2, city);
            statementFlagTrue.addBatch();

            statementReconcile.setString(1, city);
            statementReconcile.addBatch();
        }

        statementInsert.executeBatch();
        statementUpdate.executeBatch();
        statementFlagFalse.executeBatch();
        statementFlagTrue.executeBatch();
        statementReconcile.executeBatch();

        connection.commit();
    }

}
