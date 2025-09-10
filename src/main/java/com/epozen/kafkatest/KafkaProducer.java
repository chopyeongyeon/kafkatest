package com.epozen.kafkatest;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Kafka에 메시지를 전송하는 프로듀서 서비스 클래스
 * <p>
 * 지정된 토픽으로 문자열 메시지를 전송할 수 있음
 * </p>
 * 
 * @author chopyeongyeon
 */
@Service
public class KafkaProducer {
	
    /**
     * KafkaTemplate은 Kafka 프로듀서를 래핑한 템플릿으로, 메시지 전송을 편리하게 지원
     */
    private final KafkaTemplate<String, String> kafkaTemplate;
    
    /**
     * 메시지를 전송할 Kafka 토픽명
     */
    private final String topic = "choTestTopic_1";

    /**
     * KafkaProducer 생성자
     *
     * @param kafkaTemplate Spring Kafka 템플릿 빈 주입
     */
    public KafkaProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * 지정된 메시지를 Kafka 토픽으로 전송
     *
     * @param message 전송할 메시지 문자열
     */
    public void sendMessage(String message) {
        kafkaTemplate.send(topic, message);
    }

}
