import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by liyazhou on 2017/3/17.
 */
public class TestSparkOracle {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .master("local").appName("TestSparkOracle")
                .config("spark.cores.max", "1")
                .config("spark.executor.cores", "1")
                .config("spark.executor.memory", "1g")
                .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
        JavaRDD<String> supplierType = jsc.parallelize(Arrays.asList("supplierType")).flatMap(s -> {
            String user = "show";
            String pwd = "show";
            String url = "jdbc:oracle:thin:@(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST =192.168.1.175)(PORT =1588))(CONNECT_DATA = (SERVER = DEDICATED)(SERVICE_NAME = prodb.dhgate.com)))";
            String sql = "SELECT supplierid,identity_type FROM stat_base_supplier where 1=1 and ROWNUM < 10";
            String[] query = {"supplierid", "identity_type"};
            return getDate(url, user, pwd, sql, query).iterator();
        });
//        JavaRDD<String> test = supplierType.repartition(1);
        System.out.println(supplierType.collect());
        sparkSession.stop();


    }

    public static List<String> getDate(final String url, final String user, final String password,
                                       final String sql, final String[] query) {
        List<String> list = new ArrayList<String>();
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            try {
                Class.forName("oracle.jdbc.driver.OracleDriver");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

            conn = DriverManager.getConnection(url, user, password);
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                StringBuffer sb = new StringBuffer();
                for (int i = 0; i < query.length; i++) {
                    sb.append(rs.getString(query[i]) + "\u0001");
                }
                list.add(sb.substring(0, sb.lastIndexOf("\u0001")));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                }
            }
        }
        return list;
    }


}
