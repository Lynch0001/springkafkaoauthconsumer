package com.example.springkafkaoauthconsumer;

import com.example.springkafkaoauthconsumer.service.ConsumerService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SpringKafkaOauthConsumerApplication {

  public static void main(String[] args) {

    SpringApplication.run(SpringKafkaOauthConsumerApplication.class, args);
    ConsumerService consumerService = new ConsumerService();
    consumerService.consume();
  }

}
