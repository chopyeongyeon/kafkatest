package com.epozen.kafkatest;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Kafka 테스트용 Spring Boot 애플리케이션의 진입점(main class)
 * <p>
 * Spring Boot의 자동 설정, 컴포넌트 스캔, 설정 기능을 활성화하는
 * {@link SpringBootApplication} 어노테이션이 적용되어 있음
 * <p>
 * main() 메서드에서 SpringApplication.run()을 호출하여 애플리케이션을 실행함
 * </p>
 * 
 * @author chopyeongyeon
 */
@SpringBootApplication
public class KafkatestApplication {

    /**
     * 애플리케이션 시작 진입점
     *
     * @param args 실행 인자
     */
	public static void main(String[] args) {
		SpringApplication.run(KafkatestApplication.class, args);
	}

}
