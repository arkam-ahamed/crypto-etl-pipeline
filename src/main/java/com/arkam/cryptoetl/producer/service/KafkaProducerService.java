package com.arkam.cryptoetl.producer.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Random;

@Service
public class KafkaProducerService {

    private static final String TOPIC = "btc-transactions";
    private final Random random = new Random();

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    // Scheduled task to send a fake transaction every minute
    @Scheduled(fixedRate = 60000)
    public void sendFakeTransaction() {
        String walletId = "wallet-" + random.nextInt(1000);

        double amount = random.nextDouble() * 10;

        // Create the transaction string
        String transactionMessage = String.format("{\"walletId\": \"%s\", \"amount\": %.6f, \"timestamp\": %d}",
                walletId, amount, System.currentTimeMillis());

        // Send to Kafka topic
        kafkaTemplate.send(TOPIC, transactionMessage);
        System.out.println("Sent: " + transactionMessage);
    }
}