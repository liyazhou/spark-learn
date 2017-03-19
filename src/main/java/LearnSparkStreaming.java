/**
 * Created by liyazhou on 2017/3/18.
 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

interface JavaStreamingContextFactory extends Function0<JavaStreamingContext> {
    JavaStreamingContext create();

    @Override
    JavaStreamingContext call() throws Exception;
}

public class LearnSparkStreaming {
    public static final String appName = "LearnSparkStreaming";
    public static final String master = "local[*]";
    public static final String checkpointDirectory = "";

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(1000));
        JavaStreamingContext jssc = new JavaStreamingContext(new JavaSparkContext(conf), new Duration(1));
//        jssc.fileStream();
//        for test
//        jssc.queueStream();
//        ssc.stop();
        // Reduce function adding two integers, defined separately for clarity
        Function2<Integer, Integer, Integer> reduceFunc = new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        };
        JavaStreamingContextFactory contextFactory = new JavaStreamingContextFactory() {
            @Override
            public JavaStreamingContext create() {
//                JavaStreamingContext jssc = new JavaStreamingContext(...);  // new context
//                JavaDStream<String> lines = jssc.socketTextStream(...);     // create DStreams
                jssc.checkpoint(checkpointDirectory);                       // set checkpoint directory
                return jssc;
            }

            @Override
            public JavaStreamingContext call() throws Exception {
                return create();
            }
        };
        JavaStreamingContext context = JavaStreamingContext.getOrCreate(checkpointDirectory, contextFactory);

//        JavaPairDStream<String, Integer> pairs = ;
//// Reduce last 30 seconds of data, every 10 seconds
//        JavaPairDStream<String, Integer> windowedWordCounts = pairs.reduceByKeyAndWindow(reduceFunc, Durations.seconds(30), Durations.seconds(10));
//        List<Tuple2<String, String>> tuple2List = Arrays.asList(new Tuple2<>("liyazhou", "liyazhou"), new Tuple2<>("sdfds", "sdf"));
//        JavaPairRDD<String, String> dataset = jssc.sparkContext().parallelizePairs(tuple2List);
//        JavaPair
//        JavaPairDStream<String, String> windowedStream = stream.window(Durations.seconds(20));
//        JavaPairDStream<String, String> joinedStream = windowedStream.transform(
//                new Function<JavaRDD<Tuple2<String, String>>, JavaRDD<Tuple2<String, String>>>() {
//                    @Override
//                    public JavaRDD<Tuple2<String, String>> call(JavaRDD<Tuple2<String, String>> rdd) {
//                        return rdd.join(dataset);
//                    }
//                }
//        );
    }
}

