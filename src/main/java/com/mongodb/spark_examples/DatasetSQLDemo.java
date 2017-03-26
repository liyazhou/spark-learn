package com.mongodb.spark_examples;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;


public final class DatasetSQLDemo {

    public static void main(final String[] args) throws InterruptedException {

        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
                .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.spark")
                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.spark")
                .getOrCreate();

        // Create a JavaSparkContext using the SparkSession's SparkContext object
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());

        // Load data and infer schema, disregard toDF() name as it returns Dataset
        Dataset<Row> implicitDS = MongoSpark.load(jsc).toDF();
        implicitDS.printSchema();
        implicitDS.show();

        // Load data with explicit schema
        // Create a custom ReadConfig
        Map<String, String> readOverrides = new HashMap<String, String>();
        readOverrides.put("collection", "characterDemo");
        readOverrides.put("readPreference.name", "secondaryPreferred");
        ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
        Dataset<CharacterDemo> explicitDS = MongoSpark.load(jsc, readConfig).toDS(CharacterDemo.class);
        explicitDS.printSchema();
        explicitDS.show();

        // Create the temp view and execute the query
        explicitDS.createOrReplaceTempView("characters");
        Dataset<Row> centenarians = sparkSession.sql("SELECT name, age FROM characters WHERE age >= 100");
        centenarians.show();

        // Write the data to the "hundredClub" collection
        MongoSpark.write(centenarians).option("collection", "hundredClub").mode("overwrite").save();

        // Load the data from the "hundredClub" collection
        MongoSpark.load(sparkSession, ReadConfig.create(sparkSession).withOption("collection", "hundredClub"), CharacterDemo.class).show();

        jsc.close();

    }
}