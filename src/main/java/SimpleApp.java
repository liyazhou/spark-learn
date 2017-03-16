/**
 * Created by liyazhou on 2017/3/15.
 */
    /* SimpleApp.java */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
//    ./bin/spark-submit --class "SimpleApp" --master spark://127.0.0.1:7077 /Users/liyazhou/workspace/spark-learn/target/spark-learn-1.0-SNAPSHOT.jar
//    ./bin/spark-submit --class "SimpleApp" --master spark://192.168.112.245:7077 /Users/liyazhou/workspace/spark-learn/target/spark-learn-1.0-SNAPSHOT.jar
//    ./bin/spark-submit --class "SimpleApp" --master spark://192.168.222.55:7077 /Users/liyazhou/workspace/spark-learn/target/spark-learn-1.0-SNAPSHOT.jar
//    把jar上传到master 所在的服务器路径
//    ./bin/spark-submit --class "SimpleApp" --master spark://192.168.222.55:7077 spark-learn-1.0-SNAPSHOT.jar

public class SimpleApp {
    public static void main(String[] args) {
        String logFile = "./README.md"; // Should be some file on your system
        SparkConf conf = new SparkConf().setAppName("Simple Application");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> logData = sc.textFile(logFile).cache();

        long numAs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) {
                return s.contains("a");
            }
        }).count();

        long numBs = logData.filter((Function<String, Boolean>) s -> s.contains("b")).count();

        long numCs = logData.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return s.contains("c");
            }
        }).count();
        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs + ", lines with c: " + numCs);

        sc.stop();
    }
}
