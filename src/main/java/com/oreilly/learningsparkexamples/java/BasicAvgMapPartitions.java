/**
 * Illustrates a simple map partitions in Java to compute the average
 */
package com.oreilly.learningsparkexamples.java;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

//import org.eclipse.jetty.client.ContentExchange;
//import org.eclipse.jetty.client.HttpClient;

public final class BasicAvgMapPartitions implements Serializable {
    public static void main(String[] args) throws Exception {
        String master;
        if (args.length > 0) {
            master = args[0];
        } else {
            master = "local";
        }
        BasicAvgMapPartitions bamp = new BasicAvgMapPartitions();
        bamp.run(master);
    }

    public void run(String master) {
        JavaSparkContext sc = new JavaSparkContext(
                master, "basicavgmappartitions", System.getenv("SPARK_HOME"), System.getenv("JARS"));
        JavaRDD<Integer> rdd = sc.parallelize(
                Arrays.asList(1, 2, 3, 4, 5));
        FlatMapFunction<Iterator<Integer>, AvgCount> setup = (FlatMapFunction<Iterator<Integer>, AvgCount>) input -> {
            AvgCount a = new AvgCount(0, 0);
            while (input.hasNext()) {
                a.total_ += input.next();
                a.num_ += 1;
            }
            ArrayList<AvgCount> ret = new ArrayList<>();
            ret.add(a);
            return ret.iterator();
        };
        Function2<AvgCount, AvgCount, AvgCount> combine = (Function2<AvgCount, AvgCount, AvgCount>) (a, b) -> {
            a.total_ += b.total_;
            a.num_ += b.num_;
            return a;
        };

        AvgCount result = rdd.mapPartitions(setup).reduce(combine);
        System.out.println(result.avg());
    }

    class AvgCount implements Serializable {
        public Integer total_;
        public Integer num_;

//        public AvgCount merge(Iterable<Integer> input) {
//            for (Integer elem : input) {
//                num_ += 1;
//                total_ += elem;
//            }
//            return this;
//        }

        public AvgCount() {
            total_ = 0;
            num_ = 0;
        }

        public AvgCount(Integer total, Integer num) {
            total_ = total;
            num_ = num;
        }

        public float avg() {
            return total_ / (float) num_;
        }
    }
}
