/**
 * Illustrates a simple map in Java
 */
package com.oreilly.learningsparkexamples.java;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.eclipse.jetty.client.ContentExchange;
import org.eclipse.jetty.client.HttpClient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

public class BasicMapPartitions {
    public static void main(String[] args) throws Exception {
        String master;
        if (args.length > 0) {
            master = args[0];
        } else {
            master = "local[*]";
        }
        JavaSparkContext sc = new JavaSparkContext(
                master, "basicmappartitions", System.getenv("SPARK_HOME"), System.getenv("JARS"));
        sc.setLogLevel("ERROR");
        JavaRDD<String> rdd = sc.parallelize(
                Arrays.asList("KK6JKQ", "Ve3UoW", "kk6jlk", "W6BB", "W7BB"));

        JavaRDD<String> result = rdd.mapPartitions(
                (FlatMapFunction<Iterator<String>, String>) input -> {
                    ArrayList<String> content = new ArrayList<String>();
                    ArrayList<ContentExchange> cea = new ArrayList<ContentExchange>();
                    HttpClient client = new HttpClient();
                    try {
                        client.start();
                        while (input.hasNext()) {
                            ContentExchange exchange = new ContentExchange(true);
//                            exchange.setURL("http://qrzcq.com/call/" + input.next());
                            exchange.setURL("https://www.baidu.com/s?wd=" + input.next());
                            client.send(exchange);
                            cea.add(exchange);
                        }
                        for (ContentExchange exchange : cea) {
                            exchange.waitForDone();
                            content.add(exchange.getResponseContent());
                        }
                    } catch (Exception e) {
                    }
                    return content.iterator();
                });
        System.out.println(StringUtils.join(result.collect(), ","));

        JavaRDD<String> test = rdd.mapPartitions(input -> {
            ArrayList<String> content = new ArrayList<>();
            ArrayList<ContentExchange> cea = new ArrayList<>();
            HttpClient httpClient = new HttpClient();
            httpClient.start();
            try {
                while (input.hasNext()) {
                    String data = input.next();
                    System.out.println("---------" + data + "  " + "id: " + TaskContext.getPartitionId());
                    ContentExchange contentExchange = new ContentExchange(true);
                    contentExchange.setURL("http://www.baidu.com/" + data);
                    httpClient.send(contentExchange);
                    cea.add(contentExchange);
                }
                for (ContentExchange exchange : cea) {
                    exchange.waitForDone();
                    content.add(exchange.getResponseContent());
                }
            } catch (Exception e) {

            }
            return content.iterator();
        }).cache();
        System.out.println(test.partitions().size());
        System.out.println(test.count());
        System.out.println(StringUtils.join(test.collect(), ","));
    }
}
