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
import reactor.core.scheduler.Schedulers;
import com.example.service.NativeClickHouseExcelExportService;
import com.example.service.JdbcClickHouseQueryService;
import java.nio.file.Path;

@RestController
@RequestMapping("/api/clickhouse")
public class ClickHouseQueryController {

    private final ClickHouseQueryService queryService;
    private final NativeClickHouseExcelExportService excelExportService;
    private final JdbcClickHouseQueryService jdbcService;

    public ClickHouseQueryController(ClickHouseQueryService queryService, NativeClickHouseExcelExportService excelExportService, JdbcClickHouseQueryService jdbcService) {
        this.queryService = queryService;
        this.excelExportService = excelExportService;
        this.jdbcService = jdbcService;
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
			if (res instanceof com.example.service.ClickHouseQueryService.QueryResult) {
				com.example.service.ClickHouseQueryService.QueryResult qr = (com.example.service.ClickHouseQueryService.QueryResult) res;
				body.put("rows", qr.rows);
			}
			if (res instanceof Long) {
				Long bytes = (Long) res;
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

	@PostMapping("/packets/distinct")
	public Mono<ResponseEntity<Map<String, Object>>> fetchPacketsDistinct(@RequestBody Map<String, Object> payload) {
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
			? queryService.fetchPacketsDistinctStreamTiming(from, to, subPktType == null || subPktType.isBlank() ? null : subPktType, 100000, 0)
			: queryService.fetchPacketsDistinct(from, to, subPktType == null || subPktType.isBlank() ? null : subPktType, limit, offset);

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
			if (res instanceof com.example.service.ClickHouseQueryService.QueryResult) {
				com.example.service.ClickHouseQueryService.QueryResult qr = (com.example.service.ClickHouseQueryService.QueryResult) res;
				body.put("rows", qr.rows);
			}
			if (res instanceof Long) {
				Long bytes = (Long) res;
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

	@PostMapping("/packets/distinct-columns")
	public Mono<ResponseEntity<Map<String, Object>>> fetchDistinctPerColumn(@RequestBody Map<String, Object> payload) {
		String fromStr = payload.getOrDefault("from", "").toString();
		String toStr = payload.getOrDefault("to", "").toString();
		String subPktType = payload.getOrDefault("type", "").toString();
		Integer perColumnLimit = payload.containsKey("perColumnLimit") ? ((Number) payload.get("perColumnLimit")).intValue() : null;

		LocalDateTime from = parseDateTimeFlexible("from", fromStr);
		LocalDateTime to = parseDateTimeFlexible("to", toStr);
		if (!to.isAfter(from)) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Parameter 'to' must be after 'from'");
		}

		long start = System.currentTimeMillis();
		return queryService.fetchDistinctPerColumn(from, to, subPktType == null || subPktType.isBlank() ? null : subPktType, perColumnLimit)
			.map(map -> {
				long duration = System.currentTimeMillis() - start;
				Map<String, Object> body = new java.util.LinkedHashMap<>();
				body.put("from", from.toString());
				body.put("to", to.toString());
				if (subPktType != null && !subPktType.isBlank()) {
					body.put("type", subPktType);
				}
				if (perColumnLimit != null) body.put("perColumnLimit", perColumnLimit);
				body.put("durationMs", duration);
				body.put("columns", map);
				body.put("success", true);
				return ResponseEntity.ok(body);
			}).onErrorResume(ex -> {
				Map<String, Object> body = new java.util.LinkedHashMap<>();
				body.put("from", from.toString());
				body.put("to", to.toString());
				if (subPktType != null && !subPktType.isBlank()) {
					body.put("type", subPktType);
				}
				if (perColumnLimit != null) body.put("perColumnLimit", perColumnLimit);
				body.put("durationMs", System.currentTimeMillis() - start);
				body.put("success", false);
				body.put("error", ex.getMessage());
				return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(body));
			});
	}

	@PostMapping("/packets/distinct-authorized-speed")
	public Mono<ResponseEntity<Map<String, Object>>> fetchDistinctAuthorizedSpeed(@RequestBody Map<String, Object> payload) {
		String fromStr = payload.getOrDefault("from", "").toString();
		String toStr = payload.getOrDefault("to", "").toString();
		String subPktType = payload.getOrDefault("type", "").toString();
		Integer limit = payload.containsKey("limit") ? ((Number) payload.get("limit")).intValue() : null;

		LocalDateTime from = parseDateTimeFlexible("from", fromStr);
		LocalDateTime to = parseDateTimeFlexible("to", toStr);
		if (!to.isAfter(from)) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Parameter 'to' must be after 'from'");
		}

		long start = System.currentTimeMillis();
		return queryService.fetchDistinctAuthorizedSpeed(from, to, subPktType == null || subPktType.isBlank() ? null : subPktType, limit)
			.map(values -> {
				long duration = System.currentTimeMillis() - start;
				Map<String, Object> body = new java.util.LinkedHashMap<>();
				body.put("from", from.toString());
				body.put("to", to.toString());
				if (subPktType != null && !subPktType.isBlank()) {
					body.put("type", subPktType);
				}
				if (limit != null) body.put("limit", limit);
				body.put("durationMs", duration);
				body.put("authorized_speed", values);
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
			body.put("durationMs", System.currentTimeMillis() - start);
			body.put("success", false);
			body.put("error", ex.getMessage());
			return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(body));
		});
	}

    @PostMapping("/packets/stream-timing-unlimited")
    public Mono<ResponseEntity<Map<String, Object>>> fetchPacketsStreamTimingUnlimited(@RequestBody Map<String, Object> payload) {
		String fromStr = payload.getOrDefault("from", "").toString();
		String toStr = payload.getOrDefault("to", "").toString();
		String subPktType = payload.getOrDefault("type", "").toString();

		LocalDateTime from = parseDateTimeFlexible("from", fromStr);
		LocalDateTime to = parseDateTimeFlexible("to", toStr);
		if (!to.isAfter(from)) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Parameter 'to' must be after 'from'");
        }
        long start = System.currentTimeMillis();
        return queryService.fetchPacketsStreamTimingUnlimited(from, to, subPktType == null || subPktType.isBlank() ? null : subPktType)
            .map(bytesTransferred -> {
                long duration = System.currentTimeMillis() - start;
                Map<String, Object> body = new java.util.LinkedHashMap<>();
                body.put("from", from.toString());
                body.put("to", to.toString());
                if (subPktType != null && !subPktType.isBlank()) {
                    body.put("type", subPktType);
                }
                body.put("durationMs", duration);
                body.put("bytesTransferred", bytesTransferred);
                body.put("streamingMode", true);
                body.put("unlimitedMode", true);
                body.put("timeoutMinutes", 60);
                body.put("optimizationSettings", Map.of(
                    "maxMemoryUsage", "8GB",
                    "maxMemoryForUser", "12GB", 
                    "maxThreads", 8,
                    "maxBlockSize", "64KB",
                    "preferredBlockSize", "1MB",
                    "maxIoOperations", 10,
                    "externalSortThreshold", "4GB",
                    "joinMemoryLimit", "2GB"
                ));
                body.put("success", true);
                return ResponseEntity.ok(body);
            }).onErrorResume(ex -> {
                Map<String, Object> body = new java.util.LinkedHashMap<>();
                body.put("from", from.toString());
                body.put("to", to.toString());
                if (subPktType != null && !subPktType.isBlank()) {
                    body.put("type", subPktType);
                }
                body.put("durationMs", System.currentTimeMillis() - start);
                body.put("streamingMode", true);
                body.put("unlimitedMode", true);
                body.put("timeoutMinutes", 60);
                body.put("optimizationSettings", Map.of(
                    "maxMemoryUsage", "8GB",
                    "maxMemoryForUser", "12GB", 
                    "maxThreads", 8,
                    "maxBlockSize", "64KB",
                    "preferredBlockSize", "1MB",
                    "maxIoOperations", 10,
                    "externalSortThreshold", "4GB",
                    "joinMemoryLimit", "2GB"
                ));
                body.put("success", false);
                body.put("error", ex.getMessage());
                return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(body));
            });
    }

    @PostMapping("/packets/export-excel-native")
    public Mono<ResponseEntity<Map<String, Object>>> exportExcelNative(@RequestBody Map<String, Object> payload) {
        String fromStr = payload.getOrDefault("from", "").toString();
        String toStr = payload.getOrDefault("to", "").toString();
        String subPktType = payload.getOrDefault("type", "").toString();
        Integer maxRowsPerSheet = payload.containsKey("maxRowsPerSheet") ? ((Number) payload.get("maxRowsPerSheet")).intValue() : 1_048_576;
        Integer rowWindowInMemory = payload.containsKey("rowWindowInMemory") ? ((Number) payload.get("rowWindowInMemory")).intValue() : 300;

        LocalDateTime from = parseDateTimeFlexible("from", fromStr);
        LocalDateTime to = parseDateTimeFlexible("to", toStr);
        if (!to.isAfter(from)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Parameter 'to' must be after 'from'");
        }

        long start = System.currentTimeMillis();
        return Mono.fromCallable(() -> excelExportService.exportToExcel(from, to, subPktType == null || subPktType.isBlank() ? null : subPktType, maxRowsPerSheet, rowWindowInMemory))
            .subscribeOn(Schedulers.boundedElastic())
            .map(paths -> {
                long duration = System.currentTimeMillis() - start;
                Map<String, Object> body = new java.util.LinkedHashMap<>();
                body.put("from", from.toString());
                body.put("to", to.toString());
                if (subPktType != null && !subPktType.isBlank()) {
                    body.put("type", subPktType);
                }
                body.put("durationMs", duration);
                java.util.List<String> files = new java.util.ArrayList<>();
                for (Path p : paths) files.add(p.toAbsolutePath().toString());
                body.put("files", files);
                body.put("success", true);
                return ResponseEntity.ok(body);
            }).onErrorResume(ex -> {
                Map<String, Object> body = new java.util.LinkedHashMap<>();
                body.put("from", from.toString());
                body.put("to", to.toString());
                if (subPktType != null && !subPktType.isBlank()) {
                    body.put("type", subPktType);
                }
                body.put("durationMs", System.currentTimeMillis() - start);
                body.put("success", false);
                body.put("error", ex.getMessage());
                return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(body));
            });
    }

    // =================== JDBC-native endpoints (server defaults only) ===================

    @PostMapping("/native/packets")
    public Mono<ResponseEntity<Map<String, Object>>> nativeFetchPackets(@RequestBody Map<String, Object> payload) {
        String fromStr = payload.getOrDefault("from", "").toString();
        String toStr = payload.getOrDefault("to", "").toString();
        String subPktType = payload.getOrDefault("type", "").toString();
        Integer limit = payload.containsKey("limit") ? ((Number) payload.get("limit")).intValue() : null;
        Integer offset = payload.containsKey("offset") ? ((Number) payload.get("offset")).intValue() : null;
        LocalDateTime from = parseDateTimeFlexible("from", fromStr);
        LocalDateTime to = parseDateTimeFlexible("to", toStr);
        long start = System.currentTimeMillis();
        return Mono.fromCallable(() -> jdbcService.fetchPackets(from, to, subPktType == null || subPktType.isBlank() ? null : subPktType, limit, offset))
            .subscribeOn(Schedulers.boundedElastic())
            .map(rows -> {
                Map<String, Object> body = new java.util.LinkedHashMap<>();
                body.put("from", from.toString());
                body.put("to", to.toString());
                if (subPktType != null && !subPktType.isBlank()) body.put("type", subPktType);
                if (limit != null) body.put("limit", limit);
                if (offset != null) body.put("offset", offset);
                body.put("durationMs", System.currentTimeMillis() - start);
                body.put("rows", rows);
                return ResponseEntity.ok(body);
            });
    }

    @PostMapping("/native/packets/count")
    public Mono<ResponseEntity<Map<String, Object>>> nativeFetchCount(@RequestBody Map<String, Object> payload) {
        String fromStr = payload.getOrDefault("from", "").toString();
        String toStr = payload.getOrDefault("to", "").toString();
        String subPktType = payload.getOrDefault("type", "").toString();
        LocalDateTime from = parseDateTimeFlexible("from", fromStr);
        LocalDateTime to = parseDateTimeFlexible("to", toStr);
        long start = System.currentTimeMillis();
        return Mono.fromCallable(() -> jdbcService.fetchPacketsCount(from, to, subPktType == null || subPktType.isBlank() ? null : subPktType))
            .subscribeOn(Schedulers.boundedElastic())
            .map(cnt -> {
                Map<String, Object> body = new java.util.LinkedHashMap<>();
                body.put("from", from.toString());
                body.put("to", to.toString());
                if (subPktType != null && !subPktType.isBlank()) body.put("type", subPktType);
                body.put("durationMs", System.currentTimeMillis() - start);
                body.put("count", cnt);
                return ResponseEntity.ok(body);
            });
    }

    @PostMapping("/native/packets/distinct")
    public Mono<ResponseEntity<Map<String, Object>>> nativeFetchDistinct(@RequestBody Map<String, Object> payload) {
        String fromStr = payload.getOrDefault("from", "").toString();
        String toStr = payload.getOrDefault("to", "").toString();
        String subPktType = payload.getOrDefault("type", "").toString();
        Integer limit = payload.containsKey("limit") ? ((Number) payload.get("limit")).intValue() : null;
        Integer offset = payload.containsKey("offset") ? ((Number) payload.get("offset")).intValue() : null;
        LocalDateTime from = parseDateTimeFlexible("from", fromStr);
        LocalDateTime to = parseDateTimeFlexible("to", toStr);
        long start = System.currentTimeMillis();
        return Mono.fromCallable(() -> jdbcService.fetchDistinct(from, to, subPktType == null || subPktType.isBlank() ? null : subPktType, limit, offset))
            .subscribeOn(Schedulers.boundedElastic())
            .map(rows -> {
                Map<String, Object> body = new java.util.LinkedHashMap<>();
                body.put("from", from.toString());
                body.put("to", to.toString());
                if (subPktType != null && !subPktType.isBlank()) body.put("type", subPktType);
                if (limit != null) body.put("limit", limit);
                if (offset != null) body.put("offset", offset);
                body.put("durationMs", System.currentTimeMillis() - start);
                body.put("rows", rows);
                return ResponseEntity.ok(body);
            });
    }

    @PostMapping("/native/packets/distinct-columns")
    public Mono<ResponseEntity<Map<String, Object>>> nativeFetchDistinctPerColumn(@RequestBody Map<String, Object> payload) {
        String fromStr = payload.getOrDefault("from", "").toString();
        String toStr = payload.getOrDefault("to", "").toString();
        String subPktType = payload.getOrDefault("type", "").toString();
        Integer perColumnLimit = payload.containsKey("perColumnLimit") ? ((Number) payload.get("perColumnLimit")).intValue() : null;
        LocalDateTime from = parseDateTimeFlexible("from", fromStr);
        LocalDateTime to = parseDateTimeFlexible("to", toStr);
        long start = System.currentTimeMillis();
        return Mono.fromCallable(() -> jdbcService.fetchDistinctPerColumn(from, to, subPktType == null || subPktType.isBlank() ? null : subPktType, perColumnLimit))
            .subscribeOn(Schedulers.boundedElastic())
            .map(map -> {
                Map<String, Object> body = new java.util.LinkedHashMap<>();
                body.put("from", from.toString());
                body.put("to", to.toString());
                if (subPktType != null && !subPktType.isBlank()) body.put("type", subPktType);
                if (perColumnLimit != null) body.put("perColumnLimit", perColumnLimit);
                body.put("durationMs", System.currentTimeMillis() - start);
                body.put("columns", map);
                return ResponseEntity.ok(body);
            });
    }

    @PostMapping("/native/packets/distinct-authorized-speed")
    public Mono<ResponseEntity<Map<String, Object>>> nativeFetchDistinctAuthorizedSpeed(@RequestBody Map<String, Object> payload) {
        String fromStr = payload.getOrDefault("from", "").toString();
        String toStr = payload.getOrDefault("to", "").toString();
        String subPktType = payload.getOrDefault("type", "").toString();
        Integer limit = payload.containsKey("limit") ? ((Number) payload.get("limit")).intValue() : null;
        LocalDateTime from = parseDateTimeFlexible("from", fromStr);
        LocalDateTime to = parseDateTimeFlexible("to", toStr);
        long start = System.currentTimeMillis();
        return Mono.fromCallable(() -> jdbcService.fetchDistinctAuthorizedSpeed(from, to, subPktType == null || subPktType.isBlank() ? null : subPktType, limit))
            .subscribeOn(Schedulers.boundedElastic())
            .map(values -> {
                Map<String, Object> body = new java.util.LinkedHashMap<>();
                body.put("from", from.toString());
                body.put("to", to.toString());
                if (subPktType != null && !subPktType.isBlank()) body.put("type", subPktType);
                if (limit != null) body.put("limit", limit);
                body.put("durationMs", System.currentTimeMillis() - start);
                body.put("authorized_speed", values);
                return ResponseEntity.ok(body);
            });
    }

    @PostMapping("/native/packets/stream-timing-unlimited")
    public Mono<ResponseEntity<Map<String, Object>>> nativeStreamTimingUnlimited(@RequestBody Map<String, Object> payload) {
        String fromStr = payload.getOrDefault("from", "").toString();
        String toStr = payload.getOrDefault("to", "").toString();
        String subPktType = payload.getOrDefault("type", "").toString();
        LocalDateTime from = parseDateTimeFlexible("from", fromStr);
        LocalDateTime to = parseDateTimeFlexible("to", toStr);
        long start = System.currentTimeMillis();
        return Mono.fromCallable(() -> jdbcService.streamTimingUnlimited(from, to, subPktType == null || subPktType.isBlank() ? null : subPktType))
            .subscribeOn(Schedulers.boundedElastic())
            .map(cnt -> {
                Map<String, Object> body = new java.util.LinkedHashMap<>();
                body.put("from", from.toString());
                body.put("to", to.toString());
                if (subPktType != null && !subPktType.isBlank()) body.put("type", subPktType);
                body.put("durationMs", System.currentTimeMillis() - start);
                body.put("rowCount", cnt);
                return ResponseEntity.ok(body);
            });
    }

    // Minimal timing-only streaming using HTTP-native client; returns only duration and bytes streamed
    @PostMapping("/packets/stream-timing-stats")
    public Mono<ResponseEntity<Map<String, Object>>> streamTimingStats(@RequestBody Map<String, Object> payload) {
        String fromStr = payload.getOrDefault("from", "").toString();
        String toStr = payload.getOrDefault("to", "").toString();
        String subPktType = payload.getOrDefault("type", "").toString();

        LocalDateTime from = parseDateTimeFlexible("from", fromStr);
        LocalDateTime to = parseDateTimeFlexible("to", toStr);
        long start = System.currentTimeMillis();
        return queryService.fetchPacketsStreamStatsUnlimited(from, to, subPktType == null || subPktType.isBlank() ? null : subPktType)
            .map(stats -> {
                Map<String, Object> body = new java.util.LinkedHashMap<>();
                body.put("from", from.toString());
                body.put("to", to.toString());
                if (subPktType != null && !subPktType.isBlank()) body.put("type", subPktType);
                body.put("durationMs", System.currentTimeMillis() - start);
                body.put("bytesTransferred", stats.bytesTransferred);
                body.put("rowCount", stats.rowCount);
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


