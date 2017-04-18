package com.oreilly.learningsparkexamples.java.demo;

import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by liyazhou on 2017/4/6.
 */
public class BasicJoin {
    public static void main(String[] args) {
        String master;
        if (args.length > 0) {
            master = args[0];
        } else {
            master = "local";
        }
        JavaSparkContext sc = new JavaSparkContext(
                master, "basicjoin", System.getenv("SPARK_HOME"), System.getenv("JARS"));
        sc.setLogLevel("ERROR");
        List<String> topics = Arrays.asList("dream", "fashion", "news");
        UserInfo userInfo = new UserInfo(topics);
        JavaPairRDD<Integer, UserInfo> userInfoJavaPairRDD = sc.parallelizePairs(Arrays.asList(new Tuple2(1, userInfo)))
                .partitionBy(new HashPartitioner(2))
                .persist(StorageLevel.MEMORY_ONLY());
        System.out.println(userInfoJavaPairRDD.partitioner().isPresent());
        LinkInfo linkInfo = new LinkInfo("test");
        JavaPairRDD<Integer, LinkInfo> linkInfoJavaPairRDD = sc.parallelizePairs(Arrays.asList(new Tuple2(1, linkInfo)));
        JavaPairRDD<Integer, Tuple2<UserInfo, LinkInfo>> joined = userInfoJavaPairRDD.join(linkInfoJavaPairRDD);
        JavaPairRDD<Integer, Tuple2<UserInfo, LinkInfo>> offTopicVisits =
                joined.filter(x -> !x._2()._1().getTopics().contains(x._2()._2().getTopic()));
        System.out.println(offTopicVisits.count());
        System.out.println(offTopicVisits.collect());
    }
}
