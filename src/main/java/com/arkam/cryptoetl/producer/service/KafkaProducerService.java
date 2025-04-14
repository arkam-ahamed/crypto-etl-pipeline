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

    // Scheduled task to send a fake transaction every 30 seconds
    @Scheduled(fixedRate = 30000)
    public void sendFakeTransaction() {
        String walletId = "wallet-" + random.nextInt(1000);

        boolean sendValidAmount = random.nextInt(10) < 8;
        boolean sendValidTimestamp = random.nextInt(10) < 8;

        String amount = sendValidAmount ? String.valueOf(random.nextDouble() * 10) : "\"INVALID_AMOUNT\"";
        String timestamp = sendValidTimestamp ? String.valueOf(System.currentTimeMillis()) : "\"BAD_TIMESTAMP\"";

        String transactionMessage = String.format("{\"walletId\": \"%s\", \"amount\": %s, \"timestamp\": %s}",
                walletId, amount, timestamp);

        kafkaTemplate.send(TOPIC, transactionMessage);
        System.out.println("Sent: " + transactionMessage);
    }
}