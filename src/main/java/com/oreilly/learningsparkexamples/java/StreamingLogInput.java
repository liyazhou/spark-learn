/**
 * Illustrates a simple map then filter in Java
 */
package com.oreilly.learningsparkexamples.java;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class StreamingLogInput {
    public static void main(String[] args) throws Exception {
        args = new String[]{"local[*]"};
        String master = args[0];
        JavaSparkContext sc = new JavaSparkContext(master, "StreamingLogInput");
        // Create a StreamingContext with a 1 second batch size
        JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(1));
        // Create a DStream from all the input on port 7777
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 7777);
//        JavaDStream<String> lines = jssc.socketTextStream("localhost", 7777);
        // Filter our DStream for lines with "error"
        JavaDStream<String> errorLines = lines.filter(new Function<String, Boolean>() {
            public Boolean call(String line) {
                return line.contains("error");
            }
        });
        // Print out the lines with errors, which causes this DStream to be evaluated
        System.out.println("000000000000000");
        errorLines.print();
        System.out.println("11111111111");
        System.out.println(errorLines);
        System.out.println(errorLines.count());
        System.out.println("000000000000000");
        // start our streaming context and wait for it to "finish"
        jssc.start();
        // Wait for 10 seconds then exit. To run forever call without a timeout
        jssc.awaitTermination();
//    jssc.awaitTermination(10000);
        // Stop the streaming context
//        jssc.stop();
    }

}
