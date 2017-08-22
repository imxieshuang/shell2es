package com.bonc.usdp.shell2es;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by Administrator on 2017/8/21.
 */
public class Test {
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        Class.forName("com.bonc.usdp.sql4es.jdbc.ESDriver");
        Connection con = DriverManager.getConnection("jdbc:sql4es://localhost:9302/test2?cluster.name=esfyb_cluster");
        Statement st = con.createStatement();
        // 对myidx中的mytype执行查询
        ResultSet rs = st.executeQuery("show TABLEs ");
        ResultSetMetaData rsmd = rs.getMetaData();
        int nrCols = rsmd.getColumnCount();
        // 得到像其他列信息，比如type
        while(rs.next()){
            // for(int i=1; i<=nrCols; i++){
                System.out.println(rs.getObject("TABLE_NAME"));
            // }
        }
        // rs.next();
        // System.out.println(rs);
        rs.close();
        con.close();
    }
}
