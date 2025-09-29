package com.example.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Service
public class KafkaProducerService {

	private final KafkaTemplate<String, String> kafkaTemplate;
	private final NativeConcurrentInsertService recordFactory;

	@Value("${kafka.topic:clickhouse-input}")
	private String topic;

	public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate,
	                           NativeConcurrentInsertService recordFactory) {
		this.kafkaTemplate = kafkaTemplate;
		this.recordFactory = recordFactory;
	}

	public void send(String key, String value) {
		kafkaTemplate.send(topic, key, value);
	}

	public void sendJson(Map<String, Object> record) {
		try {
			String json = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(record);
			kafkaTemplate.send(topic, json);
		} catch (Exception e) {
			throw new RuntimeException("Failed to serialize message", e);
		}
	}

	public void send6000FromFactory() {
		com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
		kafkaTemplate.executeInTransaction(kt -> {
			for (int i = 1; i <= 6000; i++) {
				Map<String, Object> record = recordFactory.createRecordData(i);
				try {
					String json = mapper.writeValueAsString(record);
					kt.send(topic, json);
				} catch (Exception e) {
					throw new RuntimeException("Failed to serialize record #" + i, e);
				}
			}
			kt.flush();
			return null;
		});
	}
}


