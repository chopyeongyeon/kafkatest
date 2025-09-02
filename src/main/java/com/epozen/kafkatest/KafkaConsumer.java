package com.epozen.kafkatest;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumer {

	@KafkaListener(topics = "choTestTopic_1", groupId = "choTestGroup")
	public void listen(String message) {
	    System.out.println("카프카 메시지 수신: " + message);
	}
	
}
