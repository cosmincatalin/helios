package com.cosminsanda;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;

import java.sql.*;

public class PostgreSink extends ForeachWriter<Row> {

    Logger logger = LogManager.getLogger(PostgreSink.class);

    private Connection connection;
    private final String url;
    private final String user;
    private final String pwd;
    private PreparedStatement statementInsert;
    private PreparedStatement statementUpdate;
    private PreparedStatement statementFlagTrue;
    private PreparedStatement statementFlagFalse;
    private PreparedStatement statementReconcile;

    public PostgreSink(String url, String user, String pwd) {
        this.url = url;
        this.user = user;
        this.pwd = pwd;
    }

    @Override
    public boolean open(long partitionId, long epochId) {
        try {
            Class.forName("org.postgresql.Driver");
            connection = DriverManager.getConnection(url, user, pwd);
            connection.setAutoCommit(false);
            statementInsert = connection.prepareStatement(
                "INSERT INTO readings" +
                    "(id, raw_xml, city, timestamp, celsius, fahrenheit, active_from, active_to, active_flag)" +
                    "VALUES(MD5(?), ?, ?, ?, ?, ?, ?, ?, ?)");
            statementUpdate = connection.prepareStatement(
                "UPDATE readings SET active_to = ?  WHERE city = ? AND timestamp = (SELECT timestamp FROM readings " +
                    "WHERE city = ? AND timestamp < ? ORDER BY timestamp DESC LIMIT 1)"
            );
            statementFlagTrue = connection.prepareStatement(
                "UPDATE readings SET active_flag = TRUE WHERE city = ? AND timestamp = (SELECT MAX(timestamp) FROM readings " +
                    "WHERE city = ?)"
            );
            statementFlagFalse = connection.prepareStatement(
                "UPDATE readings SET active_flag = FALSE  WHERE city = ? AND active_flag = TRUE"
            );
            statementReconcile = connection.prepareStatement(
                "UPDATE readings AS r1 SET active_to = " +
                    "(SELECT active_from FROM readings AS r2 WHERE r2.city = r1.city AND r2.active_from > r1.active_from ORDER BY r2.active_from LIMIT 1) " +
                    "WHERE active_from IS NOT NULL AND city = ?"
            );
        } catch (ClassNotFoundException | SQLException ex) {
            logger.error(ex);
            return false;
        }
        return true;
    }

    @Override
    public void process(Row value) {
        try {
            statementInsert.setString(1, value.getAs("raw_xml"));
            statementInsert.setString(2, value.getAs("raw_xml"));
            statementInsert.setString(3, value.isNullAt(1) ? null : value.getAs("city"));
            statementInsert.setTimestamp(4, value.isNullAt(2) ? null : value.getAs("timestamp"));
            statementInsert.setObject(5, value.isNullAt(3) ? null : value.getAs ("celsius"), Types.DOUBLE);
            statementInsert.setObject(6, value.isNullAt(4) ? null : value.getAs ("fahrenheit"), Types.DOUBLE);
            statementInsert.setTimestamp(7, value.isNullAt(2) ? null : value.getAs("timestamp"));
            statementInsert.setTimestamp(8, null);
            statementInsert.setObject(9, value.isNullAt(1) ? null : false, Types.BOOLEAN);
            statementInsert.executeUpdate();

            if (!value.isNullAt(1)) {
                statementUpdate.setTimestamp(1, value.getAs("timestamp"));
                statementUpdate.setString(2, value.getAs("city"));
                statementUpdate.setString(3, value.getAs("city"));
                statementUpdate.setTimestamp(4, value.getAs("timestamp"));
                statementUpdate.executeUpdate();

                statementFlagFalse.setString(1, value.getAs("city"));
                statementFlagFalse.executeUpdate();

                statementFlagTrue.setString(1, value.getAs("city"));
                statementFlagTrue.setString(2, value.getAs("city"));
                statementFlagTrue.executeUpdate();

                statementReconcile.setString(1, value.getAs("city"));
                statementReconcile.executeUpdate();
            }
        } catch (SQLException ex) {
            logger.error(ex);
        }
    }

    @Override
    public void close(Throwable errorOrNull) {
        try {
            connection.commit();
            connection.close();
        } catch (SQLException ex) {
            logger.error(ex);
        }
    }
}
