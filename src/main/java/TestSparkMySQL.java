import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.socialshops.common.StatisticsEnum;
import com.socialshops.event.saas.buyer.LoginEvent;
import com.socialshops.statistic.vo.LogStatistics;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import scala.Tuple2;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import static com.socialshops.event.EventEnum.EVENT_TYPE_MAP;

/**
 * Created by liyazhou on 2017/3/19.
 */
public class TestSparkMySQL {
    public static final SimpleDateFormat YYYYMMDD = new SimpleDateFormat("yyyy-MM-dd");

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder().appName("spark-mysql").master("local[*]").getOrCreate();
        sparkSession.sparkContext().setLogLevel("ERROR");

        testmysqlread(sparkSession);
        testmysqlwrite(sparkSession);
        testmongodb(sparkSession);

    }

    private static void testmysqlread(SparkSession sparkSession) {
        Properties connectionProperties = new Properties();
//        connectionProperties.put("user", "root");
        connectionProperties.put("user", "novoshopslog");
        connectionProperties.put("password", "novoshopslog");
//        String url = "jdbc:mysql://127.0.0.1:3306/sslog";
//        String table = "saas_event";
        String url = "jdbc:mysql://172.30.100.12:3388/novoshopslog";
        String table = "novoshops_log";
//
//        Dataset<Row> saasEventDF = sparkSession.read().jdbc(url, table, connectionProperties);
//        System.out.println(saasEventDF.rdd().getNumPartitions());
//        saasEventDF.show();
//        saasEventDF.createOrReplaceTempView("novoshops_log");
//        Dataset<Row> dd = sparkSession.sql("select * from novoshops_log");
//        dd.show();
//        Dataset<String> stringDataset = dd.map((MapFunction<Row, String>) row -> row.getAs("log_event_type"), Encoders.STRING());
//        stringDataset.show();
//        List<String> list = stringDataset.collectAsList();
//        System.out.println(list);
//
        String[] predicates = {"log_event_type='1001'", "log_event_type='1002'", "log_event_type='1003'",
                "log_event_type='1004'", "log_event_type='1005'", "log_event_type='1006'", "log_event_type='1007'",
                "log_event_type='1008'", "log_event_type='1009'"};
        Dataset<Row> datasource = sparkSession.read().jdbc(url, table, predicates, connectionProperties);
//        Dataset<Row> datasource = sparkSession.jdbc(url, table, predicates, connectionProperties);
//        println("第三种方法输出："+df2.rdd.partitions.size+","+predicates.length);
//        df2.collect().foreach(println)
        datasource.foreach(row -> {
            System.out.println(datasource.rdd().partitions());

        });


//        df.createOrReplaceTempView("novoshops_log");
//        Dataset<Row> datasetPart = sparkSession.sql("select count(*) from novoshops_log where ");
//        df.groupBy("log_event_type").count().show();
//        for (String eventType : EVENT_TYPE_MAP.keySet()) {
        String eventType = "1003";
        String currentDay = YYYYMMDD.format(new Date());
        String yesterday = YYYYMMDD.format(DateUtils.addDays(new Date(), -1));
//        String before7Day = YYYYMMDD.format(DateUtils.addDays(new Date(), -7));
//        String before30Day = YYYYMMDD.format(DateUtils.addDays(new Date(), -30));


        datasource.filter("log_date < '" + currentDay + "' and log_date >= '" + yesterday + "'").filter("log_event_type=" + eventType).foreachPartition((ForeachPartitionFunction<Row>) t -> {
            System.out.println(TaskContext.get().partitionId());
            while (t.hasNext()) {
                Row row = t.next();
                if (EVENT_TYPE_MAP.get(eventType).equals(LoginEvent.class)) {
                    String eventData = row.getAs("log_data");
                    LoginEvent loginEvent = (LoginEvent) JSON.parseObject(eventData, EVENT_TYPE_MAP.get(eventType));
                }

            }
        });
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
        JavaRDD<String> eventJavaRDD = javaSparkContext.parallelize(Lists.newArrayList(EVENT_TYPE_MAP.keySet()));
        JavaPairRDD<String, Dataset<Row>> datasetJavaPairRDD = eventJavaRDD.mapToPair(str -> {
            Dataset<Row> dateset = datasource.filter("log_date < '" + currentDay + "' and log_date >= '" + yesterday + "'").filter("log_event_type=" + str);
            Tuple2<String, Dataset<Row>> datasetTuple2 = new Tuple2<String, Dataset<Row>>(str, dateset);
            return datasetTuple2;
        });
        JavaPairRDD<String, String> datasetJavaPairRDD2 = eventJavaRDD.mapToPair(str -> {
            Dataset<Row> dateset = datasource.filter("log_date < '" + currentDay + "' and log_date >= '" + yesterday + "'").filter("log_event_type=" + str);
//            Tuple2<String, Dataset<Row>> datasetTuple2 = new Tuple2<String, Dataset<Row>>(str, dateset);
            String eventData = dateset.map((MapFunction<Row, String>) row -> row.getAs("log_data"), Encoders.STRING()).toString();
            Tuple2<String, String> datasetTuple2 = new Tuple2<String, String>(str, eventData);
            return datasetTuple2;
        });

        Dataset<Row> dateset = datasource.filter("log_date < '" + currentDay + "' and log_date >= '" + yesterday + "'").filter("log_event_type=" + eventType);

        Dataset<String> eventData = dateset.map((MapFunction<Row, String>) row -> row.getAs("log_data"), Encoders.STRING());
        if (EVENT_TYPE_MAP.get(eventType).equals(LoginEvent.class)) {

            JavaRDD<LoginEvent> eventDataRDD = eventData.toJavaRDD().map(data -> (LoginEvent) JSON.parseObject(data, EVENT_TYPE_MAP.get(eventType)));
            JavaPairRDD<String, Integer> result = eventDataRDD.mapToPair(loginEvent -> new Tuple2<>(loginEvent.getMobileAppId(), 1)).reduceByKey((v1, v2) -> v1 + v2);

//            Dataset<LoginEvent> loginEventDataset = eventData.map((MapFunction<String, LoginEvent>) data -> (LoginEvent) JSON.parseObject(data, EVENT_TYPE_MAP.get(eventType)), Encoders.bean(LoginEvent.class));
//            loginEventDataset.groupBy("mobileAppId").count().show();
//
            JavaRDD<LogStatistics> eventCountVoJavaRDD = result.map(rr -> {
                LogStatistics logStatistics = new LogStatistics();
                logStatistics.setStatistics_mobile_app_id(rr._1());
                logStatistics.setStatistics_count(rr._2());
                logStatistics.setStatistics_date(new Timestamp(YYYYMMDD.parse(yesterday).getTime()));
                logStatistics.setStatistics_name(StatisticsEnum.login.name());
                return logStatistics;
            });
            Dataset<Row> eventCountVoDataset = sparkSession.createDataFrame(eventCountVoJavaRDD, LogStatistics.class);

            Properties connectionProperties2 = new Properties();
            connectionProperties2.put("user", "root");
//            connectionProperties2.put("password", "novoshopslog");
//        String url = "jdbc:mysql://127.0.0.1:3306/sslog";
//        String table = "saas_event";
            String url2 = "jdbc:mysql://127.0.0.1:3306/sslog";
            String table2 = "log_statistics";

            eventCountVoDataset.write().mode(SaveMode.Append).jdbc(url2, table2, connectionProperties2);


        }

        sparkSession.stop();

    }

    private static void testmysqlwrite(SparkSession sparkSession) {

    }

    private static void testmongodb(SparkSession sparkSession) {

    }
}
