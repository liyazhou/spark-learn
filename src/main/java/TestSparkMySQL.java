import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Properties;

/**
 * Created by liyazhou on 2017/3/19.
 */
public class TestSparkMySQL {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("spark-mysql").master("local[*]").getOrCreate();
        sparkSession.sparkContext().setLogLevel("ERROR");

        testmysqlread(sparkSession);
        testmysqlwrite(sparkSession);
        testmongodb(sparkSession);

    }

    private static void testmysqlread(SparkSession sparkSession) {
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "root");
//        connectionProperties.put("passsword", NULL);
        String url = "jdbc:mysql://127.0.0.1:3306/sslog";
        String table = "saas_event";

        Dataset<Row> saasEventDF = sparkSession.read().jdbc(url, table, connectionProperties);
        saasEventDF.show();
        saasEventDF.createOrReplaceTempView("saas_event");
        Dataset<Row> dd = sparkSession.sql("select * from saas_event");
        dd.show();
        Dataset<String> stringDataset = dd.map((MapFunction<Row, String>) row -> row.getAs("logData"), Encoders.STRING());
        stringDataset.show();
        List<String> list = stringDataset.collectAsList();
        System.out.println(list);

    }

    private static void testmysqlwrite(SparkSession sparkSession) {

    }

    private static void testmongodb(SparkSession sparkSession) {

    }
}
