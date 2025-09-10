package com.epozen.kafkatest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.springframework.web.bind.annotation.*;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;

import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;

/**
 * Kafka 관련 다양한 Admin API 및 메시지 전송 기능을 제공하는 REST 컨트롤러 클래스.
 * <p>
 * Kafka 클러스터에서 토픽 목록, 파티션 수, Under-replicated Partitions, 컨슈머 그룹 활성 컨슈머 수 등을 조회할 수 있습니다.
 * 메시지 전송 기능도 포함되어 있습니다.
 * </p>
 * 
 * @author chopyeongyeon
 */
@RestController
@RequestMapping("/kafka")
public class KafkaTestController {

    private final KafkaProducer kafkaProducer;
    private final AdminClient adminClient;

    /**
     * KafkaTestController 생성자
     *
     * @param kafkaProducer Kafka 메시지 전송용 프로듀서 빈
     * @param adminClient Kafka AdminClient 빈
     */
    public KafkaTestController(KafkaProducer kafkaProducer, AdminClient adminClient) {
        this.kafkaProducer = kafkaProducer;
        this.adminClient = adminClient;
    }
    
    /**
     * 클러스터 내 등록된 모든 토픽 이름 목록을 조회
     *
     * @return 토픽 이름 리스트. 조회 실패시 빈 리스트 반환
     */
    @GetMapping("/topics")
    public List<String> getTopicList() {
    	
        try {
        	
            // listTopics() 호출 후 Future에서 결과를 가져오고 토픽명 리스트 반환
            return new ArrayList<>(adminClient.listTopics().names().get());
            
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            return Collections.emptyList();
        }
        
    }
    
    /**
     * 특정 토픽의 파티션 개수를 조회
     *
     * @param topicName 조회할 토픽 이름
     * @return 토픽 이름과 파티션 수 문자열, 예외 발생 시 에러 메시지 반환
     */
    @GetMapping("/topics/{topicName}")
    public String getPartitionCount(@PathVariable("topicName") String topicName) {
    	
        try {
        	
            DescribeTopicsResult result = adminClient.describeTopics(Collections.singletonList(topicName));
            TopicDescription topicDescription = result.topicNameValues().get(topicName).get();
            int partitionCount = topicDescription.partitions().size();
            
            return "Topic: " + topicName + ", Partition Count: " + partitionCount;
            
        } catch (InterruptedException | ExecutionException e) {
            return "Error fetching topic info: " + e.getMessage();
        }
        
    }
    
    /**
     * 모든 토픽의 Under-replicated Partitions 목록을 조회
     * <p>
     * ISR(In-Sync Replicas) 수가 전체 replicas 수보다 적은 파티션들만 필터링해서 반환
     * </p>
     *
     * @return 토픽명별 Under-replicated 파티션 리스트 맵, 없을 경우 빈 맵 반환
     */
    @GetMapping("/topics/underReplicated")
    public Map<String, List<TopicPartition>> getUnderReplicatedPartitions() {
    	
        try {
        	
            Set<String> topics = adminClient.listTopics().names().get();
            Map<String, List<TopicPartition>> urpByTopic = new HashMap<>();

            for (String topic : topics) {
            	DescribeTopicsResult descResult = adminClient.describeTopics(Collections.singleton(topic));
                Map<String, KafkaFuture<TopicDescription>> topicFutures = descResult.topicNameValues();
                TopicDescription desc = topicFutures.get(topic).get();

                List<TopicPartition> underReplicatedPartitions = new ArrayList<>();

                desc.partitions().forEach(partition -> {
                    int replicaCount = partition.replicas().size();
                    int isrCount = partition.isr().size();
                    
                    if (isrCount < replicaCount) {
                    	Map<String, Object> partitionInfo = new HashMap<>();
                        partitionInfo.put("partition", partition.partition());
                        partitionInfo.put("replicasCount", replicaCount);
                        partitionInfo.put("isrCount", isrCount);
                        partitionInfo.put("topicPartition", new TopicPartition(topic, partition.partition()));
                        underReplicatedPartitions.add(new TopicPartition(topic, partition.partition()));
                    }
                });

                if (!underReplicatedPartitions.isEmpty()) {
                    urpByTopic.put(topic, underReplicatedPartitions);
                }
                
            }
            
            return urpByTopic;
            
        } catch (InterruptedException | ExecutionException e) {
        	
            e.printStackTrace();
            return Collections.emptyMap();
            
        }
        
    }
    
    /**
     * 현재 Kafka 클러스터 내 모든 컨슈머 그룹의 활성 컨슈머 개수를 조회
     * <p>
     * 각각의 컨슈머 그룹에 연결된 활성 멤버 수를 반환
     * </p>
     *
     * @return 컨슈머 그룹 ID별 활성 컨슈머 수 맵, 조회 실패 시 빈 맵 반환
     */
    @GetMapping("/consumergroups/activeConsumers")
    public Map<String, Integer> getActiveConsumerCounts() {
    	
        try {
            // 클러스터 내 모든 컨슈머 그룹 목록 조회
        	Collection<ConsumerGroupListing> groupListings = adminClient.listConsumerGroups().valid().get();

            // 그룹 ID 리스트 생성
        	List<String> groupIds = groupListings.stream()
                    .map(group -> group.groupId())
                    .collect(Collectors.toList());

            // describeConsumerGroups 호출
            DescribeConsumerGroupsResult describeResult = adminClient.describeConsumerGroups(groupIds);

            // 그룹별 ConsumerGroupDescription 맵
            Map<String, ConsumerGroupDescription> descriptions = describeResult.all().get();

            // 그룹별 활성 컨슈머 수 매핑
            Map<String, Integer> activeConsumerCounts = new HashMap<>();

            for (Map.Entry<String, ConsumerGroupDescription> entry : descriptions.entrySet()) {
                ConsumerGroupDescription desc = entry.getValue();
                // members()가 활성 컨슈머 리스트
                int activeConsumerCount = desc.members().size();
                activeConsumerCounts.put(entry.getKey(), activeConsumerCount);
            }

            return activeConsumerCounts;

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            return Collections.emptyMap();
        }
        
    }
    
    /**
     * 지정된 메시지를 10,000회 Kafka로 전송하는 API
     *
     * @param message 전송할 메시지 문자열
     */
    @PostMapping("/send")
    public void sendMessage(@RequestParam("message") String message) {
    	
    	for(int i=0; i<10000; i++) {
    		kafkaProducer.sendMessage(message);
    	}
        
    }
	
}
