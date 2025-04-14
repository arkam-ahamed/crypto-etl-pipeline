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

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
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
                        System.out.println("Invalid amount, skipping: " + msg);
                        return;
                    }

                    long timestamp;
                    try {
                        timestamp = json.getLong("timestamp");
                    } catch (Exception e) {
                        System.out.println("Invalid timestamp, skipping: " + msg);
                        return;
                    }

                    // Build JSON object for Elasticsearch
                    JSONObject cleanJson = new JSONObject();
                    cleanJson.put("walletId", walletId);
                    cleanJson.put("amount", amount);
                    cleanJson.put("timestamp", timestamp);

                    // Send to Elasticsearch
                    sendToElasticsearch(cleanJson.toString());

                } catch (Exception ex) {
                    System.out.println("Skipping invalid JSON: " + msg);
                }
            });
        });

        // Start the streaming context
        jssc.start();
        jssc.awaitTermination();
    }

    private static void sendToElasticsearch(String jsonPayload) {
        try {
            URL url = new URL("http://localhost:9200/btc-transactions/_doc");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setRequestProperty("Content-Type", "application/json");

            try (OutputStream os = conn.getOutputStream()) {
                byte[] input = jsonPayload.getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }

            int code = conn.getResponseCode();
            System.out.println("🔥 Received message: " + jsonPayload);
            if (code == 201 || code == 200) {
                System.out.println("✅ Sent to Elasticsearch: " + jsonPayload);
            } else {
                System.out.println("❌ Failed to send to Elasticsearch. Code: " + code);
            }

            conn.disconnect();
        } catch (Exception e) {
            System.out.println("Exception sending to Elasticsearch: " + e.getMessage());
        }
    }
}
