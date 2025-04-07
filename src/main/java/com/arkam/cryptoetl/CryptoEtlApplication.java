package com.arkam.cryptoetl;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class CryptoEtlApplication {

	public static void main(String[] args) {
		SpringApplication.run(CryptoEtlApplication.class, args);
	}

}
