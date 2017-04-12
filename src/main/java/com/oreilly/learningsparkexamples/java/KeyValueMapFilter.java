/**
 * Illustrates how to make a PairRDD then do a basic filter
 */
package com.oreilly.learningsparkexamples.java;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Map;
import java.util.Map.Entry;

public final class KeyValueMapFilter {

    public static void main(String[] args) throws Exception {
        args = new String[]{"local", "./README.md"};
        if (args.length != 2) {
            throw new Exception("Usage KeyValueMapFilter sparkMaster inputFile");
        }
        String master = args[0];
        String inputFile = args[1];

        JavaSparkContext sc = new JavaSparkContext(
                master, "KeyValueMapFilter", System.getenv("SPARK_HOME"), System.getenv("JARS"));
        JavaRDD<String> input = sc.textFile(inputFile);
        PairFunction<String, String, String> keyData = new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String x) {
                return new Tuple2(x.split(" ")[0], x);
            }
        };
        Function<Tuple2<String, String>, Boolean> longWordFilter = new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> input) {
                return (input._2().length() < 30);
            }
        };
        JavaPairRDD<String, String> rdd = input.mapToPair(keyData);
        JavaPairRDD<String, String> result = rdd.filter(longWordFilter);
        Map<String, String> resultMap = result.collectAsMap();
        for (Entry<String, String> entry : resultMap.entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }
    }
}
