package com.epozen.kafkatest;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * Kafka 메시지를 수신하는 소비자(Consumer) 서비스 클래스
 * <p>
 * 지정한 Kafka 토픽을 구독하며 메시지를 수신할 때마다 로그에 출력
 * </p>
 * 
 * @author chopyeongyeon
 */
@Service
public class KafkaConsumer {

    /**
     * 지정된 Kafka 토픽(choTestTopic_1)에서 메시지를 수신하면 호출되는 리스너 메서드
     *
     * @param message Kafka로부터 수신된 메시지 문자열
     */
	@KafkaListener(topics = "choTestTopic_1", groupId = "choTestGroup")
	public void listen(String message) {
	    System.out.println("카프카 메시지 수신: " + message);
	}
	
}
