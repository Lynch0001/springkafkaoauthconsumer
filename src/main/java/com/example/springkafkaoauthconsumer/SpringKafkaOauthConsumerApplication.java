package com.example.springkafkaoauthproducer;

import com.example.springkafkaoauthproducer.service.ProducerService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SpringKafkaOauthProducerApplication {

  public static void main(String[] args) {

    SpringApplication.run(SpringKafkaOauthProducerApplication.class, args);
    ProducerService producerService = new ProducerService();
    producerService.produce();
  }

}
