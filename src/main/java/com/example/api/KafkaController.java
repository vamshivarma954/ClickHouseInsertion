package com.example.api;

import com.example.service.KafkaProducerService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/kafka")
public class KafkaController {

	private final KafkaProducerService producerService;

	public KafkaController(KafkaProducerService producerService) {
		this.producerService = producerService;
	}

	@PostMapping("/publish")
	public ResponseEntity<String> publish(@RequestBody Map<String, Object> record) {
		producerService.sendJson(record);
		return ResponseEntity.ok("queued");
	}

	@PostMapping("/publish-batch")
	public ResponseEntity<String> publishBatch(@RequestBody List<Map<String, Object>> records) {
		for (Map<String, Object> r : records) {
			producerService.sendJson(r);
		}
		return ResponseEntity.ok("queued:" + records.size());
	}

	@PostMapping("/publish-6000")
	public ResponseEntity<String> publish6000() {
		producerService.send6000FromFactory();
		return ResponseEntity.ok("queued:6000");
	}
}


