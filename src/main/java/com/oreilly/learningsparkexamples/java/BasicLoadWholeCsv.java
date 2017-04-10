/**
 * Illustrates joining two csv files
 */
package com.oreilly.learningsparkexamples.java;

import au.com.bytecode.opencsv.CSVReader;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BasicLoadWholeCsv {

    public static void main(String[] args) throws Exception {
        args = new String[]{"local", "files/favourite_animals.csv", "files/result", "spark"};
        if (args.length != 4) {
            throw new Exception("Usage BasicLoadCsv sparkMaster csvInputFile csvOutputFile key");
        }
        String master = args[0];
        String csvInput = args[1];
        String outputFile = args[2];
        final String key = args[3];

        JavaSparkContext sc = new JavaSparkContext(
                master, "loadwholecsv", System.getenv("SPARK_HOME"), System.getenv("JARS"));
        JavaPairRDD<String, String> csvData = sc.wholeTextFiles(csvInput);
        JavaRDD<String[]> keyedRDD = csvData.flatMap(new ParseLine());
        List<String> list = keyedRDD.flatMap(x -> {
            List<String> result = new ArrayList<String>();
            for (String y : x) {
                result.add(y);
            }
            return result.iterator();
        }).collect();
        System.out.println(list);
        JavaRDD<String[]> result =
                keyedRDD.filter(new Function<String[], Boolean>() {
                    public Boolean call(String[] input) {
                        return input[0].equals(key);
                    }
                });

        result.saveAsTextFile(outputFile);
//        result.saveAsObjectFile(outputFile);
    }

    public static class ParseLine implements FlatMapFunction<Tuple2<String, String>, String[]> {
        public Iterator<String[]> call(Tuple2<String, String> file) throws Exception {
            CSVReader reader = new CSVReader(new StringReader(file._2()));
            return reader.readAll().iterator();
        }
    }
}
