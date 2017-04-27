/**
 * Illustrates using counters and broadcast variables for chapter 6
 */
package com.oreilly.learningsparkexamples.java;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.eclipse.jetty.client.ContentExchange;
import org.eclipse.jetty.client.HttpClient;
import scala.Tuple2;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ChapterSixExample {
    public static void main(String[] args) throws Exception {
        args = new String[]{"local", "files/callsigns", "files/call_signs2.txt", "files/result"};

        if (args.length != 4) {
            throw new Exception("Usage AccumulatorExample sparkMaster inputFile outDirectory");
        }
        String sparkMaster = args[0];
        String inputFile = args[1];
        String inputFile2 = args[2];
        String outputDir = args[3];

        JavaSparkContext sc = new JavaSparkContext(
                sparkMaster, "ChapterSixExample", System.getenv("SPARK_HOME"), System.getenv("JARS"));
        JavaRDD<String> rdd = sc.textFile(inputFile);
        // Count the number of lines with KK6JKQ
        final Accumulator<Integer> count = sc.accumulator(0);
        rdd.foreach(new VoidFunction<String>() {
            public void call(String line) {
                if (line.contains("KK6JKQ")) {
                    count.add(1);
                }
            }
        });
        System.out.println("Lines with 'KK6JKQ': " + count.value());
        // Create Accumulators initialized at 0
        final Accumulator<Integer> blankLines = sc.accumulator(0);
        JavaRDD<String> callSigns = rdd.flatMap(
                new FlatMapFunction<String, String>() {
                    public Iterator<String> call(String line) {
                        if (line.equals("")) {
                            blankLines.add(1);
                        }
                        return Arrays.asList(line.split(" ")).iterator();
                    }
                });
        callSigns.saveAsTextFile(outputDir + "/callsigns");
        System.out.println("Blank lines: " + blankLines.value());
        // Start validating the call signs
        final Accumulator<Integer> validSignCount = sc.accumulator(0);
        final Accumulator<Integer> invalidSignCount = sc.accumulator(0);
        JavaRDD<String> validCallSigns = callSigns.filter(
                new Function<String, Boolean>() {
                    public Boolean call(String callSign) {
                        Pattern p = Pattern.compile("\\A\\d?\\p{Alpha}{1,2}\\d{1,4}\\p{Alpha}{1,3}\\Z");
                        Matcher m = p.matcher(callSign);
                        boolean b = m.matches();
                        if (b) {
                            validSignCount.add(1);
                        } else {
                            invalidSignCount.add(1);
                        }
                        return b;
                    }
                });
        JavaPairRDD<String, Integer> contactCounts = validCallSigns.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String callSign) {
                        return new Tuple2(callSign, 1);
                    }
                }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer x, Integer y) {
                return x + y;
            }
        });
        // Force evaluation so the counters are populated
        contactCounts.count();
        if (invalidSignCount.value() < 0.1 * validSignCount.value()) {
            contactCounts.saveAsTextFile(outputDir + "/contactCount");
        } else {
            System.out.println("Too many errors " + invalidSignCount.value() +
                    " for " + validSignCount.value());
            System.exit(1);
        }
        // Read in the call sign table
        // Lookup the countries for each call sign in the
        // contactCounts RDD.
        final Broadcast<String[]> signPrefixes = sc.broadcast(loadCallSignTable());
        JavaPairRDD<String, Integer> countryContactCounts = contactCounts.mapToPair(
                new PairFunction<Tuple2<String, Integer>, String, Integer>() {
                    public Tuple2<String, Integer> call(Tuple2<String, Integer> callSignCount) {
                        String sign = callSignCount._1();
                        String country = lookupCountry(sign, signPrefixes.value());
                        return new Tuple2(country, callSignCount._2());
                    }
                }).reduceByKey(new SumInts());
        countryContactCounts.saveAsTextFile(outputDir + "/countries.txt");
        System.out.println("Saved country contact counts as a file");
        // Use mapPartitions to re-use setup work.
        JavaPairRDD<String, CallLog[]> contactsContactLists = validCallSigns.mapPartitionsToPair(
                new PairFlatMapFunction<Iterator<String>, String, CallLog[]>() {
                    public Iterator<Tuple2<String, CallLog[]>> call(Iterator<String> input) {
                        // List for our results.
                        ArrayList<Tuple2<String, CallLog[]>> callsignQsos =
                                new ArrayList<Tuple2<String, CallLog[]>>();
                        ArrayList<Tuple2<String, ContentExchange>> requests =
                                new ArrayList<Tuple2<String, ContentExchange>>();
                        ObjectMapper mapper = createMapper();
                        HttpClient client = new HttpClient();
                        try {
                            client.start();
                            while (input.hasNext()) {
                                requests.add(createRequestForSign(input.next(), client));
                            }
                            for (Tuple2<String, ContentExchange> signExchange : requests) {
                                callsignQsos.add(fetchResultFromRequest(mapper, signExchange));
                            }
                        } catch (Exception e) {
                        }
                        return callsignQsos.iterator();
                    }
                });
        System.out.println(StringUtils.join(contactsContactLists.collect(), ","));
//        // Computer the distance of each call using an external R program
//        // adds our script to a list of files for each node to download with this job
//        String distScript = System.getProperty("user.dir") + "/src/R/finddistance.R";
//        String distScriptName = "finddistance.R";
//        sc.addFile(distScript);
//        JavaRDD<String> pipeInputs = contactsContactLists.values().map(new VerifyCallLogs()).flatMap(
//                new FlatMapFunction<CallLog[], String>() {
//                    public Iterator<String> call(CallLog[] calls) {
//                        ArrayList<String> latLons = new ArrayList<String>();
//                        for (CallLog call : calls) {
//                            latLons.add(call.mylat + "," + call.mylong +
//                                    "," + call.contactlat + "," + call.contactlong);
//                        }
//                        return latLons.iterator();
//                    }
//                });
//        JavaRDD<String> distances = pipeInputs.pipe(SparkFiles.get(distScriptName));
//        // First we need to convert our RDD of String to a DoubleRDD so we can
//        // access the stats function
//        JavaDoubleRDD distanceDoubles = distances.mapToDouble(new DoubleFunction<String>() {
//            public double call(String value) {
//                return Double.parseDouble(value);
//            }
//        });
//        final StatCounter stats = distanceDoubles.stats();
//        final Double stddev = stats.stdev();
//        final Double mean = stats.mean();
//        JavaDoubleRDD reasonableDistances =
//                distanceDoubles.filter(new Function<Double, Boolean>() {
//                    public Boolean call(Double x) {
//                        return (Math.abs(x - mean) < 3 * stddev);
//                    }
//                });
//        System.out.println(StringUtils.join(reasonableDistances.collect(), ","));
        sc.stop();
        System.exit(0);
    }

    static CallLog[] readExchangeCallLog(ObjectMapper mapper, ContentExchange exchange) {
        try {
            exchange.waitForDone();
            String responseJson = exchange.getResponseContent();
            return mapper.readValue(responseJson, CallLog[].class);
        } catch (Exception e) {
            return new CallLog[0];
        }
    }

    static Tuple2<String, CallLog[]> fetchResultFromRequest(ObjectMapper mapper,
                                                            Tuple2<String, ContentExchange> signExchange) {
        String sign = signExchange._1();
        ContentExchange exchange = signExchange._2();
        return new Tuple2(sign, readExchangeCallLog(mapper, exchange));
    }

    static Tuple2<String, ContentExchange> createRequestForSign(String sign, HttpClient client) throws Exception {
        ContentExchange exchange = new ContentExchange(true);
//        http://new73s.herokuapp.com/qsos/KK6JKQ.json
        exchange.setURL("http://new73s.herokuapp.com/qsos/" + sign + ".json");
        client.send(exchange);
        return new Tuple2(sign, exchange);
    }

    static ObjectMapper createMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return mapper;
    }

    static String[] loadCallSignTable() throws FileNotFoundException {
        Scanner callSignTbl = new Scanner(new File("./files/callsign_tbl_sorted"));
        ArrayList<String> callSignList = new ArrayList<String>();
        while (callSignTbl.hasNextLine()) {
            callSignList.add(callSignTbl.nextLine());
        }
        return callSignList.toArray(new String[0]);
    }

    static String lookupCountry(String callSign, String[] table) {
        Integer pos = Arrays.binarySearch(table, callSign);
        if (pos < 0) {
            pos = -pos - 1;
        }
        return table[pos].split(",")[1];
    }

    public static class SumInts implements Function2<Integer, Integer, Integer> {
        public Integer call(Integer x, Integer y) {
            return x + y;
        }
    }

    public static class VerifyCallLogs implements Function<CallLog[], CallLog[]> {
        public CallLog[] call(CallLog[] input) {
            ArrayList<CallLog> res = new ArrayList<CallLog>();
            if (input != null) {
                for (CallLog call : input) {
                    if (call != null && call.mylat != null && call.mylong != null
                            && call.contactlat != null && call.contactlong != null) {
                        res.add(call);
                    }
                }
            }
            return res.toArray(new CallLog[0]);
        }
    }
}
