package com.oreilly.learningsparkexamples.java;

import org.apache.spark.HashPartitioner;
import org.apache.spark.Partition;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by liyazhou on 2017/4/6.
 */
public class BasicPartitioner {
    public static void main(String[] args) {
        String master;
        if (args.length > 0) {
            master = args[0];
        } else {
            master = "local";
        }
        JavaSparkContext sc = new JavaSparkContext(
                master, "basicpartitioner", System.getenv("SPARK_HOME"), System.getenv("JARS"));

        JavaPairRDD<Integer, Integer> javaPairRDD = sc.parallelizePairs(
                Arrays.asList(new Tuple2(1, 1), new Tuple2(2, 2), new Tuple2(3, 3)));
        System.out.println(javaPairRDD.partitioner().isPresent());
        System.out.println(javaPairRDD.partitioner());
        JavaPairRDD<Integer, Integer> test = javaPairRDD.partitionBy(new HashPartitioner(2));
        System.out.println(javaPairRDD.partitioner().isPresent());
        System.out.println(javaPairRDD.partitioner());
        System.out.println(test.partitioner().isPresent());
        System.out.println(test.partitioner());
        if (test.partitioner().isPresent()) {
            Iterator<Partition> iterator = test.partitions().iterator();
            while (iterator.hasNext()) {
                System.out.println(iterator.next().toString());
            }
        }
    }
}
