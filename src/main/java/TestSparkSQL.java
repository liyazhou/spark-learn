/**
 * Created by liyazhou on 2017/3/19.
 */

import org.apache.spark.sql.SparkSession;

public class TestSparkSQL {
    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder().master("local[*]")
                .appName("Java Spark SQL basic example")
//                .config("spark.some.config.option", "some-value")
                .getOrCreate();

    }
}
