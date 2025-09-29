package com.example.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class KafkaConsumerService {

	private final ClickHouseBulkInsertService clickHouseBulkInsertService;

	public KafkaConsumerService(ClickHouseBulkInsertService clickHouseBulkInsertService) {
		this.clickHouseBulkInsertService = clickHouseBulkInsertService;
	}

	@KafkaListener(topics = "${kafka.topic:clickhouse-input}", containerFactory = "kafkaListenerContainerFactory")
	public void onBatch(List<ConsumerRecord<String, String>> records, Acknowledgment ack) {
		if (records == null || records.isEmpty()) {
			ack.acknowledge();
			return;
		}

		List<Map<String, Object>> batch = new ArrayList<>(records.size());
		for (ConsumerRecord<String, String> rec : records) {
			try {
				Map<String, Object> map = new com.fasterxml.jackson.databind.ObjectMapper().readValue(rec.value(), HashMap.class);
				batch.add(map);
			} catch (Exception ignored) {
			}
		}

		// Process in chunks of 6000 to guarantee large bulk inserts, but also handle smaller batches
		int from = 0;
		final int CHUNK = 6000;
		try {
			while (from < batch.size()) {
				int to = Math.min(from + CHUNK, batch.size());
				clickHouseBulkInsertService.insertJsonEachRow(batch.subList(from, to));
				from = to;
			}
			ack.acknowledge();
		} catch (Exception e) {
			throw new RuntimeException("Failed bulk insert to ClickHouse", e);
		}
	}
}



