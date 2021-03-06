/**
 * Illustrates loading a json file and finding out if people like pandas
 */
package com.oreilly.learningsparkexamples.java;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.Iterator;

public class BasicLoadJson {

    public static void main(String[] args) throws Exception {
        args = new String[]{"local", "files/pandainfo.json", "files/result"};
        if (args.length != 3) {
            throw new Exception("Usage BasicLoadJson [sparkMaster] [jsoninput] [jsonoutput]");
        }
        String master = args[0];
        String fileName = args[1];
        String outfile = args[2];

        JavaSparkContext sc = new JavaSparkContext(
                master, "basicloadjson", System.getenv("SPARK_HOME"), System.getenv("JARS"));
        JavaRDD<String> input = sc.textFile(fileName);
        JavaRDD<Person> result = input.mapPartitions(new ParseJson()).filter(new LikesPandas());
//        System.out.println(result.count());
//        System.out.println(result.collect());
        JavaRDD<String> formatted = result.mapPartitions(new WriteJson());
        formatted.saveAsTextFile(outfile);
    }

    public static class Person implements java.io.Serializable {
        public String name;
        public Boolean lovesPandas;

        @Override
        public String toString() {
            return name + ":" + lovesPandas;
        }
    }

    public static class ParseJson implements FlatMapFunction<Iterator<String>, Person> {
        public Iterator<Person> call(Iterator<String> lines) throws Exception {
            ArrayList<Person> people = new ArrayList<Person>();
            ObjectMapper mapper = new ObjectMapper();
            while (lines.hasNext()) {
                String line = lines.next();
                try {
                    people.add(mapper.readValue(line, Person.class));
                } catch (Exception e) {
                    // Skip invalid input
                }
            }
            return people.iterator();
        }
    }

    public static class LikesPandas implements Function<Person, Boolean> {
        public Boolean call(Person person) {
            return person.lovesPandas;
        }
    }

    public static class WriteJson implements FlatMapFunction<Iterator<Person>, String> {
        public Iterator<String> call(Iterator<Person> people) throws Exception {
            ArrayList<String> text = new ArrayList<String>();
            ObjectMapper mapper = new ObjectMapper();
            while (people.hasNext()) {
                Person person = people.next();
                text.add(mapper.writeValueAsString(person));
            }
            return text.iterator();
        }
    }
}
