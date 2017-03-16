import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by liyazhou on 2017/3/15.
 */
public class TestApp {
    //    public static final String spark_master = "spark://192.168.222.55:7077";
    public static final String spark_master = "spark://127.0.0.1:7077";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Simple Application");
        conf.setMaster(spark_master);
        conf.setAppName("TestApp");

        conf.setJars(new String[]{"/Users/liyazhou/workspace/spark-learn/target/spark-learn-1.0-SNAPSHOT.jar"});
        JavaSparkContext sc = new JavaSparkContext(conf);
//
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
//        JavaRDD<Integer> javaRDD = sc.parallelize(data);
//
//        System.out.println(javaRDD.collect());
//        System.out.println("function");
//        System.out.println(javaRDD.map((Function<Integer, Integer>) v1 -> v1)
//                .reduce((Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2));
//        System.out.println("map reduce");
//        System.out.println(javaRDD.map(v1 -> v1).reduce((a, b) -> a + b));
//        System.out.println("direct reduce");
//        System.out.println(javaRDD.reduce((a, b) -> a + b));
//        System.out.println(javaRDD.filter((Function<Integer,Boolean>) f -> f % 2 == 0).count());
//        System.out.println(javaRDD.filter((Function<Integer,Boolean>) v1 -> v1 % 2 == 0).collect());
//

//        JavaRDD<String> lines = sc.textFile("/Users/liyazhou/workspace/spark-learn/README.md");
//        JavaRDD<Integer> lineLengths = lines.map(s -> s.length());
//        System.out.println(lineLengths.collect());
//        int totalLength = lineLengths.reduce((a, b) -> a + b);
//        System.out.println(totalLength);

//
//        JavaRDD<String> lines2 = sc.textFile("");
//        JavaRDD<Integer> linesLen2 = lines2.map(s -> s.length());
//        int total = lineLengths.reduce((a, b) -> a+b);

//        lineLengths.persist(StorageLevel.MEMORY_ONLY());µm

//        final int[] counter = {0};
//        JavaRDD<Integer> rdd = sc.parallelize(data);
//
//// Wrong: Don't do this!!
//        rdd.foreach(x -> counter[0] += x);
//
//        System.out.println("Counter value: " + counter[0]);
//
//        LongAccumulator accum = sc.sc().longAccumulator();
//        sc.parallelize(Arrays.asList(1, 2, 3, 4)).foreach(x -> accum.add(x));
////        accum.value();
//        System.out.println("accumulator" + accum.value());

        JavaRDD<String> lines = sc.textFile("/Users/liyazhou/workspace/spark-learn/README.md");
//        JavaPairRDD<String, Integer> pairs = lines.mapToPair(s -> new Tuple2(s, 1));
//        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);
//        System.out.println(counts.collect());
//
//        JavaPairRDD<String, Integer> counts  = lines
//                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
//                .mapToPair(word -> new Tuple2<>(word, 1))
//                .reduceByKey((a, b) -> a + b);
//        System.out.println(counts.collect());
//        System.out.println("test");
        JavaRDD<String> javaRDD = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());

//        System.out.println(javaRDD.collect());
        Map<String, Long> j = javaRDD.mapToPair(word -> new Tuple2<>(word, 1)).countByKey();
        System.out.println(j);

        Broadcast<int[]> broadcastVar = sc.broadcast(new int[]{1, 2, 3});
        broadcastVar.value();
        // returns [1, 2, 3]
        Broadcast<int[]> broadcast = sc.broadcast(new int[]{1});
        broadcast.value();


        LongAccumulator accum = sc.sc().longAccumulator();

        sc.parallelize(Arrays.asList(1, 2, 3, 4)).foreach(x -> accum.add(x));
// ...
// 10/09/29 18:41:08 INFO SparkContext: Tasks finished in 0.317106 s

        accum.value();
// returns 10

//        LongAccumulator accum2 = sc.sc().longAccumulator();
//        data.map(x -> { accum2.add(x); return f(x); });
// Here, accum is still 0 because no actions have caused the `map` to be computed.

    }
/*
        // Then, create an Accumulator of this type:
        VectorAccumulatorV2 myVectorAcc = new VectorAccumulatorV2();
// Then, register it into spark context:
        sc.sc().register(myVectorAcc, "MyVectorAcc1");
    //    public static final String spark_master = "spark://192.168.222.55:7077";
//    每次更改重新改生成jar包
    public static final String spark_master = "spark://127.0.0.1:7077";
    class VectorAccumulatorV2 implements AccumulatorV2<MyVector, MyVector> {

        private MyVector myVector = MyVector.createZeroVector();

        public void reset() {
            myVector.reset();
        }

        public void add(MyVector v) {
            myVector.add(v);
        }
    }
*/

}
