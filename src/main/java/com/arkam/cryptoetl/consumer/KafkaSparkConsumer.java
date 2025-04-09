package com.arkam.cryptoetl.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Map;

public class KafkaSparkConsumer {

    public static void main(String[] args) throws Exception {
        // Create JavaSparkContext and JavaStreamingContext
        SparkConf conf = new SparkConf().setAppName("KafkaSparkConsumer").setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

        // Set Kafka parameters
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "btc-transactions-consumer");
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // Create a direct stream to read messages from Kafka
        JavaDStream<String> messages = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(java.util.Collections.singleton("btc-transactions"), kafkaParams)
        ).map(record -> record.value().toString());

        messages.foreachRDD(rdd -> {
            rdd.foreach(msg -> {
                try {
                    JSONObject json = new JSONObject(msg);

                    String walletId = json.getString("walletId");

                    double amount;
                    try {
                        amount = json.getDouble("amount");
                    } catch (Exception e) {
                        amount = 0.0;
                    }

                    long timestamp;
                    try {
                        timestamp = json.getLong("timestamp");
                    } catch (Exception e) {
                        timestamp = System.currentTimeMillis();
                    }

                    System.out.println("Cleaned -> walletId: " + walletId + ", amount: " + amount + ", timestamp: " + timestamp);
                } catch (Exception ex) {
                    System.out.println("Skipping invalid JSON: " + msg);
                }
            });
        });

        messages.print();

        // Start the streaming context
        jssc.start();
        jssc.awaitTermination();
    }
}
