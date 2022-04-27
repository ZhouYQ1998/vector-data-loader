package edu.zju.gis.zyq.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author Zhou Yuquan
 * @version 1.0, 2021-11-20
 */
public class PGHelper {

    // 数据库设置
    private String url;
    private String username;
    private String password;

    public PGHelper(String url, String username, String password) {
        this.url = url;
        this.username = username;
        this.password = password;
    }

    public void run(String sql) {
        try {
            Class.forName("org.postgresql.Driver").newInstance();
            Connection con = DriverManager.getConnection(this.url, this.username, this.password);
            Statement st = con.createStatement();
            st.execute(sql);
            st.close();
            con.close();
        } catch (SQLException | ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
        }
    }

}
