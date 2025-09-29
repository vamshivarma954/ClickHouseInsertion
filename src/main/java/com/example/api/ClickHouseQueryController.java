package com.example.api;

import com.example.service.ClickHouseQueryService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Map;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/clickhouse")
public class ClickHouseQueryController {

	private final ClickHouseQueryService queryService;

	public ClickHouseQueryController(ClickHouseQueryService queryService) {
		this.queryService = queryService;
	}



	@PostMapping("/packets")
	public Mono<ResponseEntity<Map<String, Object>>> fetchPacketsPost(@RequestBody Map<String, Object> payload) {
		String fromStr = payload.getOrDefault("from", "").toString();
		String toStr = payload.getOrDefault("to", "").toString();
		String subPktType = payload.getOrDefault("type", "").toString();
		Integer limit = payload.containsKey("limit") ? ((Number) payload.get("limit")).intValue() : null;
		Integer offset = payload.containsKey("offset") ? ((Number) payload.get("offset")).intValue() : null;

		LocalDateTime from = parseDateTimeFlexible("from", fromStr);
		LocalDateTime to = parseDateTimeFlexible("to", toStr);
		if (!to.isAfter(from)) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Parameter 'to' must be after 'from'");
		}

		long start = System.currentTimeMillis();
		boolean noDataMode = (limit == null && offset == null);
		Mono<?> exec = noDataMode
			? queryService.fetchPacketsStreamTiming(from, to, subPktType == null || subPktType.isBlank() ? null : subPktType, 100000, 0)
			: queryService.fetchPackets(from, to, subPktType == null || subPktType.isBlank() ? null : subPktType, limit, offset);

		return exec.map(res -> {
			long duration = System.currentTimeMillis() - start;
			Map<String, Object> body = new java.util.LinkedHashMap<>();
			body.put("from", from.toString());
			body.put("to", to.toString());
			if (subPktType != null && !subPktType.isBlank()) {
				body.put("type", subPktType);
			}
			if (limit != null) body.put("limit", limit);
			if (offset != null) body.put("offset", offset);
			body.put("durationMs", duration);
			if (res instanceof com.example.service.ClickHouseQueryService.QueryResult qr) {
				body.put("rows", qr.rows);
			}
			if (res instanceof Long bytes) {
				body.put("bytesTransferred", bytes);
			}
			body.put("success", true);
			return ResponseEntity.ok(body);
		}).onErrorResume(ex -> {
			Map<String, Object> body = new java.util.LinkedHashMap<>();
			body.put("from", from.toString());
			body.put("to", to.toString());
			if (subPktType != null && !subPktType.isBlank()) {
				body.put("type", subPktType);
			}
			if (limit != null) body.put("limit", limit);
			if (offset != null) body.put("offset", offset);
			body.put("durationMs", System.currentTimeMillis() - start);
			body.put("success", false);
			body.put("error", ex.getMessage());
			return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(body));
		});
	}

	@PostMapping("/packets/count")
	public Mono<ResponseEntity<Map<String, Object>>> fetchPacketsCount(@RequestBody Map<String, Object> payload) {
		String fromStr = payload.getOrDefault("from", "").toString();
		String toStr = payload.getOrDefault("to", "").toString();
		String subPktType = payload.getOrDefault("type", "").toString();

		LocalDateTime from = parseDateTimeFlexible("from", fromStr);
		LocalDateTime to = parseDateTimeFlexible("to", toStr);
		if (!to.isAfter(from)) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Parameter 'to' must be after 'from'");
		}

		long start = System.currentTimeMillis();
		return queryService.fetchPacketsCount(from, to, subPktType == null || subPktType.isBlank() ? null : subPktType)
			.map(cnt -> {
				long duration = System.currentTimeMillis() - start;
				Map<String, Object> body = new java.util.LinkedHashMap<>();
				body.put("from", from.toString());
				body.put("to", to.toString());
				if (subPktType != null && !subPktType.isBlank()) {
					body.put("type", subPktType);
				}
				body.put("durationMs", duration);
				body.put("count", cnt);
				return ResponseEntity.ok(body);
			});
	}

	private static LocalDateTime parseDateTimeFlexible(String paramName, String value) {
		if (value == null || value.trim().isEmpty()) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Missing required parameter '" + paramName + "'");
		}
		String v = value.trim().replace('T', ' ');
		DateTimeFormatter[] patterns = new DateTimeFormatter[] {
			DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
			DateTimeFormatter.ISO_LOCAL_DATE_TIME
		};
		for (DateTimeFormatter p : patterns) {
			try {
				return LocalDateTime.parse(v, p);
			} catch (DateTimeParseException ignored) { }
		}
		throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
				"Invalid date-time for '" + paramName + "'. Use 'yyyy-MM-dd HH:mm:ss' or ISO 'yyyy-MM-ddTHH:mm:ss'");
	}
}


