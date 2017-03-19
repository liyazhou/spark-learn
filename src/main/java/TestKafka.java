/**
 * Created by liyazhou on 2017/3/18.
 */

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class TestKafka {
    public static void main(String[] args) throws Exception {
        Map<String, Object> kafkaParams = new HashMap<>();
//        kafkaParams.put("bootstrap.servers", "localhost:9092,anotherhost:9092");
        kafkaParams.put("bootstrap.servers", "192.168.222.52:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
//        参数的配置
//        kafkaParams.put("auto.offset.reset", "latest");
//        kafkaParams.put("enable.auto.commit", false);

//        Collection<String> topics = Arrays.asList("test-topic", "topicB");
        Collection<String> topics = Arrays.asList("test-topic");

        // Create context with a 2 seconds batch interval
        SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount").setMaster("local[*]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));
        final JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );

//        JavaPairDStream<String, String> javaPairDStream = stream.mapToPair(
//                record -> new Tuple2<>(record.key(), record.value()));
//        JavaDStream<String> javaDStream = stream.map(v1 -> {
//            System.out.println("^^^^^^^^^^^^^^^^^^");
//            System.out.println(Thread.currentThread().getId());
//            System.out.println(TaskContext.getPartitionId());
//            return v1.value();});
////        javaPairDStream.print();
//        javaDStream.print();
//
//// Import dependencies and create kafka params as in Create Direct Stream above
//
//        OffsetRange[] offsetRanges = {
//                // topic, partition, inclusive starting offset, exclusive ending offset
//                OffsetRange.create("test", 0, 0, 100),
//                OffsetRange.create("test", 1, 0, 100)
//        };
//
//        JavaRDD<ConsumerRecord<String, String>> rdd = KafkaUtils.createRDD(
//                jssc.sparkContext(),
//                kafkaParams,
//                offsetRanges,
//                LocationStrategies.PreferConsistent()
//        );
        stream.foreachRDD((rdd1 -> {
            final OffsetRange[] offsetRanges1 = ((HasOffsetRanges) rdd1.rdd()).offsetRanges();
            rdd1.foreachPartition(consumerRecords -> {
                System.out.println(TaskContext.get().partitionId());
                OffsetRange o = offsetRanges1[TaskContext.get().partitionId()];
                System.out.println(
                        o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
            });
        }));
        jssc.start();
        jssc.awaitTermination();
    }
}
