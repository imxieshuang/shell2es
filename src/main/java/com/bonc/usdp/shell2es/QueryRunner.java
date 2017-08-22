package com.bonc.usdp.shell2es;

import io.airlift.log.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

/**
 * Created by Administrator on 2017/8/21.
 */
public class QueryRunner implements Closeable {
    private String host;
    private String port;
    private String index;
    private String clusterName;
    private static Logger logger = Logger.get(QueryRunner.class);
    private Connection connection = null;
    private Statement statement = null;

    public QueryRunner(String host, String port, String index, String clusterName) {
        this.host = host;
        this.port = port;
        this.index = index;
        this.clusterName = clusterName;
        String url = format("jdbc:sql4es://%s:%s/%s?cluster.name=%s", host, port, index, clusterName);
        try {
            Class.forName("com.bonc.usdp.sql4es.jdbc.ESDriver");
            connection = DriverManager.getConnection(url);
            statement = connection.createStatement();
        } catch (ClassNotFoundException | SQLException e) {
            logger.error(e.getMessage());
        }


    }

    public QueryResults run(String sql) {
        ResultSetMetaData rsmd = null;
        try {
            ResultSet rs = statement.executeQuery(sql);
            rsmd = rs.getMetaData();
            int nrCols = rsmd.getColumnCount();
            // 得到像其他列信息，比如type
            ArrayList<List<Object>> data = new ArrayList<>();
            ArrayList<String> columnsName = new ArrayList<>();

            for (int i = 1; i < nrCols; i++) {
                columnsName.add(rsmd.getColumnName(i));
            }

            while (rs.next()) {
                ArrayList<Object> columns = new ArrayList<>();
                for (int i = 1; i <= nrCols; i++) {
                    columns.add(rs.getObject(i));
                }
                data.add(columns);
            }
            QueryResults queryResults = new QueryResults(data, columnsName, this);
            return queryResults;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public List<String> runMeta(String sql) {
        ResultSet rs = null;
        ArrayList<String> tablesName = new ArrayList<>();
        try {
            rs = statement.executeQuery(sql);
            while (rs.next()) {
                tablesName.add(rs.getString("TABLE_NAME"));
            }
        } catch (SQLException e) {
            logger.error(e.getMessage());
        }
        return tablesName;
    }

    public boolean isClosed() {
        try {
            return this.connection.isClosed() && this.statement.isClosed();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return true;
    }


    @Override
    public void close() throws IOException {
        if (this.statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                logger.error(e.getMessage());
            }
        }
        if (this.connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                logger.error(e.getMessage());
            }
        }
    }

    public String getHost() {
        return host;
    }

    public String getIndex() {
        return index;
    }

    public String getClusterName() {
        return clusterName;
    }

    public Connection getConnection() {
        return connection;
    }
}
