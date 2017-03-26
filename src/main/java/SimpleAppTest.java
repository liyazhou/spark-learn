/**
 * Created by liyazhou on 2017/1/22.
 */
/* SimpleAppTest.java */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class SimpleAppTest {
    //    ./spark-submit --class "SimpleAppTest" --master spark://192.168.112.245:7077 /Users/liyazhou/workspace/spark-learn/target/spark-learn-1.0-SNAPSHOT.jar
//    ./spark-submit --class "SimpleAppTest" --master spark://192.168.222.55:7077 /Users/liyazhou/workspace/spark-learn/target/spark-learn-1.0-SNAPSHOT.jar
    public static void main(String[] args) {
//        String logFile = "/Users/liyazhou/workspace/spark-learn/README.md"; // Should be some file on your system
//        String logFile = "/Users/liyazhou/workspace/spark-learn/README.md"; // Should be some file on your system
        String logFile = "file:///tmp/README.md"; // Should be some file on your system
        SparkConf conf = new SparkConf().setAppName("Simple Application");
//        conf.setMaster("spark://localhost:7077");
//        conf.setMaster("spark://localhost:7077");
        conf.setMaster("spark://192.168.222.55:7077");
        conf.setJars(new String[]{"/Users/liyazhou/workspace/spark-learn/target/spark-learn-1.0-SNAPSHOT.jar"});
        JavaSparkContext sc = new JavaSparkContext(conf);
//        sc.addJar("/Users/liyazhou/workspace/spark-learn/target/spark-learn-1.0-SNAPSHOT.jar");
        JavaRDD<String> logData = sc.textFile(logFile).cache();
        long numAs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) {
                return s.contains("a");
            }
        }).count();

        long numBs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) {
                return s.contains("b");
            }
        }).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

        sc.stop();
    }

}
