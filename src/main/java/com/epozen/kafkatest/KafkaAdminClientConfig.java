package com.epozen.kafkatest;

import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

/**
 * Kafka AdminClient를 생성하고 설정하는 Configuration 클래스
 * <p>
 * 스프링의 @Configuration으로 등록되며, application.properties의
 * Kafka bootstrap 서버 주소를 읽어 AdminClient 인스턴스를 Bean으로 제공
 * </p>
 * <p>
 * 애플리케이션 종료 시점에 AdminClient를 안전하게 종료(close)하는 메서드도 포함
 * </p>
 * 
 * @author chopyeongyeon
 */
@Configuration
public class KafkaAdminClientConfig {

    /**
     * Kafka 클러스터 bootstrap 서버 주소, application.properties에서 주입
     */
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    /**
     * AdminClient 인스턴스 참조 필드.
     */
    private AdminClient adminClient;

    /**
     * Kafka AdminClient Bean 생성 메서드
     * <p>
     * bootstrapServers 설정으로 Kafka 클러스터 연결을 위한 AdminClient를 생성하고 반환
     * 클라이언트 인스턴스는 싱글톤으로 관리
     * </p>
     *
     * @return AdminClient 인스턴스
     */
    @Bean
    public AdminClient adminClient() {
    	
        if (adminClient == null) {
            Properties props = new Properties();
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            adminClient = AdminClient.create(props);
        }
        
        return adminClient;
    }

    /**
     * 애플리케이션 종료 시 호출되는 메서드
     * <p>
     * 생성한 AdminClient 인스턴스를 안전하게 종료하여 리소스를 해제
     * </p>
     */
    @PreDestroy
    public void closeAdminClient() {
    	
        if (adminClient != null) {
            adminClient.close();
        }
        
    }
    
}
