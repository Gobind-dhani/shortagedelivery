package com.indiabulls.shortagedelivery;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class ShortagedeliveryApplication {

	public static void main(String[] args) {
		SpringApplication.run(ShortagedeliveryApplication.class, args);
	}

}
