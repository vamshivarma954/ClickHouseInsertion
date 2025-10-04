package com.example.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriComponentsBuilder;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import reactor.core.publisher.Mono;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;

@Service
public class ClickHouseQueryService {

	private final WebClient webClient;
	private final ObjectMapper mapper = new ObjectMapper();

	// ClickHouse optimization settings for 16GB RAM SSD server
	private static final long MAX_MEMORY_USAGE = 8L * 1024 * 1024 * 1024; // 8GB
	private static final long MAX_MEMORY_FOR_USER = 12L * 1024 * 1024 * 1024; // 12GB
	private static final long MAX_BYTES_BEFORE_EXTERNAL_GROUP_BY = 4L * 1024 * 1024 * 1024; // 4GB
	private static final long MAX_BYTES_BEFORE_EXTERNAL_SORT = 2L * 1024 * 1024 * 1024; // 2GB
	private static final long MAX_BYTES_IN_JOIN = 2L * 1024 * 1024 * 1024; // 2GB
	private static final long MAX_BYTES_IN_SET = 1L * 1024 * 1024 * 1024; // 1GB
	private static final long MAX_BYTES_IN_DISTINCT = 1L * 1024 * 1024 * 1024; // 1GB
	private static final int MAX_THREADS = 8;
	private static final int MAX_EXECUTION_TIME = 3600; // 1 hour
	private static final int MAX_BLOCK_SIZE = 65536; // 64KB
	private static final long PREFERRED_BLOCK_SIZE = 1024 * 1024; // 1MB
	private static final long MIN_BYTES_DIRECT_IO = 1024 * 1024; // 1MB
	private static final int MAX_IO_OPERATIONS = 10;
	private static final long MAX_READ_BUFFER_SIZE = 1024 * 1024; // 1MB
	private static final long MAX_WRITE_BUFFER_SIZE = 1024 * 1024; // 1MB

	public ClickHouseQueryService(WebClient optimizedClickHouseClient) {
		this.webClient = optimizedClickHouseClient;
	}

	@Value("${clickhouse.username}")
	private String username;

	@Value("${clickhouse.password}")
	private String password;

	@Value("${clickhouse.database}")
	private String database;

	@Value("${clickhouse.table}")
	private String table;

    public static class QueryResult {
        public final List<Map<String, Object>> rows;
        public final Double queryElapsedSeconds; // from ClickHouse JSON statistics.elapsed

        public QueryResult(List<Map<String, Object>> rows, Double queryElapsedSeconds) {
            this.rows = rows;
            this.queryElapsedSeconds = queryElapsedSeconds;
        }
    }

    public Mono<QueryResult> fetchPackets(LocalDateTime from,
                                          LocalDateTime to,
                                          String subPktType,
                                          Integer limit,
                                          Integer offset) {
        String type = (subPktType == null || subPktType.isBlank()) ? null : subPktType.trim();
        if (type != null && !type.matches("\\d{4}")) {
			throw new IllegalArgumentException("sub_pkt_type must be 4 digits");
		}
        // limit/offset may be null; buildSelectSql handles nulls (fetch all rows when absent)

		DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
		String fromStr = from.format(fmt);
		String toStr = to.format(fmt);

        String sql = buildSelectSql(fromStr, toStr, type, limit, offset);


        return webClient.post()
                .uri("/")
                .headers(h -> h.setBasicAuth(username, password))
                .contentType(MediaType.TEXT_PLAIN)
                .bodyValue(sql)
                .retrieve()
                .bodyToMono(String.class)
                .map(json -> {
                    try {
                        JsonNode root = mapper.readTree(json);
                        JsonNode data = root.get("data");
                        JsonNode stats = root.get("statistics");
                        List<Map<String, Object>> out = new ArrayList<>();
                        if (data != null && data.isArray()) {
                            for (JsonNode row : data) {
                                Map<String, Object> map = mapper.convertValue(row, HashMap.class);
                                out.add(map);
                            }
                        }
                        Double elapsed = null;
                        if (stats != null && stats.has("elapsed")) {
                            elapsed = stats.get("elapsed").asDouble();
                        }
                        return new QueryResult(out, elapsed);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to parse ClickHouse response", e);
                    }
                });
	}

    public Mono<Boolean> fetchPacketsNoData(LocalDateTime from,
                                            LocalDateTime to,
                                            String subPktType,
                                            Integer limit,
                                            Integer offset) {
        String type = (subPktType == null || subPktType.isBlank()) ? null : subPktType.trim();
        if (type != null && !type.matches("\\d{4}")) {
            return Mono.error(new IllegalArgumentException("sub_pkt_type must be 4 digits"));
        }

        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String fromStr = from.format(fmt);
        String toStr = to.format(fmt);

        String sql = buildSelectSqlNull(fromStr, toStr, type, limit, offset);

        return webClient.post()
                .uri("/")
                .headers(h -> h.setBasicAuth(username, password))
                .contentType(MediaType.TEXT_PLAIN)
                .bodyValue(sql)
                .retrieve()
                .toBodilessEntity()
                .map(ignored -> true);
    }

    public Mono<Long> fetchPacketsStreamTiming(LocalDateTime from,
                                               LocalDateTime to,
                                               String subPktType,
                                               Integer limit,
                                               Integer offset) {
        String type = (subPktType == null || subPktType.isBlank()) ? null : subPktType.trim();
        if (type != null && !type.matches("\\d{4}")) {
            return Mono.error(new IllegalArgumentException("sub_pkt_type must be 4 digits"));
        }

        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String fromStr = from.format(fmt);
        String toStr = to.format(fmt);

        // Use server-side defaults; do not send per-request SET statements
        String sql = buildSelectSqlJsonEachRow(fromStr, toStr, type, limit, offset);

        return webClient.post()
                .uri("/")
                .headers(h -> h.setBasicAuth(username, password))
                .contentType(MediaType.TEXT_PLAIN)
                .bodyValue(sql)
                .retrieve()
                .bodyToFlux(DataBuffer.class)
                .map(db -> {
                    int n = db.readableByteCount();
                    DataBufferUtils.release(db);
                    return (long) n;
                })
                .reduce(0L, Long::sum);
    }

    public Mono<Long> fetchPacketsStreamTimingUnlimited(LocalDateTime from,
                                                         LocalDateTime to,
                                                         String subPktType) {
        String type = (subPktType == null || subPktType.isBlank()) ? null : subPktType.trim();
        if (type != null && !type.matches("\\d{4}")) {
            return Mono.error(new IllegalArgumentException("sub_pkt_type must be 4 digits"));
        }

        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String fromStr = from.format(fmt);
        String toStr = to.format(fmt);

        // Use server-side defaults; do not send per-request SET statements
        String sql = buildSelectSqlJsonEachRowUnlimited(fromStr, toStr, type);

        return webClient.post()
                .uri(uriBuilder -> uriBuilder
                    .path("/")
                    .queryParam("max_execution_time", "7200")  // 2 hours server timeout
                    .queryParam("max_memory_usage", "8000000000")  // 8GB memory limit
                    .queryParam("max_result_rows", "0")  // No row limit
                    .queryParam("max_result_bytes", "0")  // No byte limit
                    .build())
                .headers(h -> h.setBasicAuth(username, password))
                .contentType(MediaType.TEXT_PLAIN)
                .bodyValue(sql)
                .exchangeToFlux(resp -> {
                    if (resp.statusCode().is2xxSuccessful()) {
                        return resp.bodyToFlux(DataBuffer.class);
                    }
                    return resp.bodyToMono(String.class)
                        .flatMapMany(body -> reactor.core.publisher.Flux.error(new RuntimeException(
                            "ClickHouse HTTP " + resp.rawStatusCode() + ": " + body
                        )));
                })
                .timeout(java.time.Duration.ofHours(1))
                .map(db -> {
                    int n = db.readableByteCount();
                    DataBufferUtils.release(db);
                    return (long) n;
                })
                .reduce(0L, Long::sum);
    }

    public static class StreamStats {
        public final long bytesTransferred;
        public final long rowCount;

        public StreamStats(long bytesTransferred, long rowCount) {
            this.bytesTransferred = bytesTransferred;
            this.rowCount = rowCount;
        }
    }

    public Mono<StreamStats> fetchPacketsStreamStatsUnlimited(LocalDateTime from,
                                                              LocalDateTime to,
                                                              String subPktType) {
        String type = (subPktType == null || subPktType.isBlank()) ? null : subPktType.trim();
        if (type != null && !type.matches("\\d{4}")) {
            return Mono.error(new IllegalArgumentException("sub_pkt_type must be 4 digits"));
        }

        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String fromStr = from.format(fmt);
        String toStr = to.format(fmt);

        String sql = buildSelectSqlJsonEachRowUnlimited(fromStr, toStr, type);

        class Acc { long bytes; long rows; }

        return webClient.post()
                .uri(uriBuilder -> uriBuilder
                    .path("/")
                    .queryParam("max_execution_time", "7200")  // 2 hours server timeout
                    .queryParam("max_memory_usage", "8000000000")  // 8GB memory limit
                    .queryParam("max_result_rows", "0")  // No row limit
                    .queryParam("max_result_bytes", "0")  // No byte limit
                    .build())
                .headers(h -> h.setBasicAuth(username, password))
                .contentType(MediaType.TEXT_PLAIN)
                .bodyValue(sql)
                .exchangeToFlux(resp -> {
                    if (resp.statusCode().is2xxSuccessful()) {
                        return resp.bodyToFlux(DataBuffer.class);
                    }
                    return resp.bodyToMono(String.class)
                        .flatMapMany(body -> reactor.core.publisher.Flux.error(new RuntimeException(
                            "ClickHouse HTTP " + resp.rawStatusCode() + ": " + body
                        )));
                })
                .timeout(java.time.Duration.ofHours(2))
                .retryWhen(reactor.util.retry.Retry.backoff(3, java.time.Duration.ofSeconds(5))
                    .filter(throwable -> {
                        String msg = throwable.getMessage();
                        return msg.contains("Connection prematurely closed") || 
                               msg.contains("Connection reset") ||
                               msg.contains("Read timeout") ||
                               msg.contains("Write timeout");
                    }))
                .map(db -> {
                    java.nio.ByteBuffer buf = db.asByteBuffer();
                    int len = buf.remaining();
                    long bytes = len;
                    long rows = 0L;
                    while (buf.hasRemaining()) {
                        if (buf.get() == (byte) '\n') rows++;
                    }
                    DataBufferUtils.release(db);
                    Acc a = new Acc();
                    a.bytes = bytes;
                    a.rows = rows;
                    return a;
                })
                .reduce(new Acc(), (a, b) -> { a.bytes += b.bytes; a.rows += b.rows; return a; })
                .map(a -> new StreamStats(a.bytes, a.rows))
                .onErrorResume(ex -> {
                    // Log the error and provide a more user-friendly message
                    System.err.println("Streaming error: " + ex.getMessage());
                    if (ex.getMessage().contains("Connection prematurely closed")) {
                        return Mono.error(new RuntimeException("Connection closed during streaming. This may be due to server timeout or network issues. Try reducing the time range or check server logs."));
                    }
                    return Mono.error(ex);
                });
    }

    public Mono<QueryResult> fetchPacketsDistinct(LocalDateTime from,
                                                  LocalDateTime to,
                                                  String subPktType,
                                                  Integer limit,
                                                  Integer offset) {
        String type = (subPktType == null || subPktType.isBlank()) ? null : subPktType.trim();
        if (type != null && !type.matches("\\d{4}")) {
            return Mono.error(new IllegalArgumentException("sub_pkt_type must be 4 digits"));
        }

        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String fromStr = from.format(fmt);
        String toStr = to.format(fmt);

        String sql = buildSelectSqlDistinct(fromStr, toStr, type, limit, offset);

        return webClient.post()
                .uri("/")
                .headers(h -> h.setBasicAuth(username, password))
                .contentType(MediaType.TEXT_PLAIN)
                .bodyValue(sql)
                .retrieve()
                .bodyToMono(String.class)
                .map(json -> {
                    try {
                        JsonNode root = mapper.readTree(json);
                        JsonNode data = root.get("data");
                        List<Map<String, Object>> out = new ArrayList<>();
                        if (data != null && data.isArray()) {
                            for (JsonNode row : data) {
                                Map<String, Object> map = mapper.convertValue(row, HashMap.class);
                                out.add(map);
                            }
                        }
                        return new QueryResult(out, null);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to parse ClickHouse DISTINCT response", e);
                    }
                });
    }

    public Mono<Long> fetchPacketsDistinctStreamTiming(LocalDateTime from,
                                                       LocalDateTime to,
                                                       String subPktType,
                                                       Integer limit,
                                                       Integer offset) {
        String type = (subPktType == null || subPktType.isBlank()) ? null : subPktType.trim();
        if (type != null && !type.matches("\\d{4}")) {
            return Mono.error(new IllegalArgumentException("sub_pkt_type must be 4 digits"));
        }

        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String fromStr = from.format(fmt);
        String toStr = to.format(fmt);

        String sql = buildSelectSqlJsonEachRowDistinct(fromStr, toStr, type, limit, offset);

        return webClient.post()
                .uri("/")
                .headers(h -> h.setBasicAuth(username, password))
                .contentType(MediaType.TEXT_PLAIN)
                .bodyValue(sql)
                .retrieve()
                .bodyToFlux(DataBuffer.class)
                .map(db -> {
                    int n = db.readableByteCount();
                    DataBufferUtils.release(db);
                    return (long) n;
                })
                .reduce(0L, Long::sum);
    }

	public Mono<Long> fetchPacketsCount(LocalDateTime from,
	                                   LocalDateTime to,
	                                   String subPktType) {
		String type = (subPktType == null || subPktType.isBlank()) ? null : subPktType.trim();
		if (type != null && !type.matches("\\d{4}")) {
			return Mono.error(new IllegalArgumentException("sub_pkt_type must be 4 digits"));
		}

		DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
		String fromStr = from.format(fmt);
		String toStr = to.format(fmt);

		StringBuilder sql = new StringBuilder();
		sql.append("SELECT count() AS cnt FROM ")
			.append(database).append('.').append(table).append(" AS t ")
			.append("PREWHERE t.message_datetime >= '").append(fromStr).append("' ")
			.append("AND t.message_datetime < '").append(toStr).append("' ");
		if (type != null) {
			sql.append("AND t.sub_pkt_type = '").append(type).append("' ");
		}
		sql.append("FORMAT JSON");

		return webClient.post()
			.uri("/")
			.headers(h -> h.setBasicAuth(username, password))
			.contentType(MediaType.TEXT_PLAIN)
			.bodyValue(sql.toString())
			.retrieve()
			.bodyToMono(String.class)
			.map(json -> {
				try {
					JsonNode root = mapper.readTree(json);
					JsonNode data = root.get("data");
					if (data != null && data.isArray() && data.size() > 0) {
						JsonNode first = data.get(0);
						JsonNode cnt = first.get("cnt");
						if (cnt != null && cnt.isNumber()) {
							return cnt.longValue();
						}
					}
					throw new RuntimeException("Unexpected ClickHouse COUNT response");
				} catch (Exception e) {
					throw new RuntimeException("Failed to parse ClickHouse COUNT response", e);
				}
			});
	}

    private String buildSelectSql(String fromStr, String toStr, String type, Integer limit, Integer offset) {
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT ")
			.append("t.message_date, t.message_time, t.stationary_kavach_id, t.message_sequence, ")
			.append("t.nms_system_id, t.system_version, t.packet_name, t.sender_identifier, ")
			.append("t.receiver_identifier, t.packet_message_length, t.frame_number, t.packet_message_sequence, ")
			.append("t.border_rfid_tag, t.onboard_kavach_identity, t.sub_pkt_type, t.sub_pkt_len_ma, ")
			.append("t.frame_offset, t.dst_loco_sos, t.train_section_type, t.line_number, t.line_name, ")
			.append("t.type_of_signal, t.signal_ov, t.stop_signal, t.current_sig_aspect, t.next_sig_aspect, ")
			.append("t.authority_type, t.approaching_signal_distance, t.authorized_speed, t.ma_wrt_sig, ")
			.append("t.req_shorten_ma, t.new_ma, t.train_length_info_sts, t.trn_len_info_type, ")
			.append("t.ref_frame_num_tlm, t.ref_offset_int_tlm, t.next_stn_comm, t.appr_stn_ilc_ibs_id, ")
			.append("t.mac_code, t.crc ")
			.append("FROM ").append(database).append(".").append(table).append(" AS t ")
			.append("PREWHERE t.message_datetime >= '").append(fromStr).append("' ")
			.append("AND t.message_datetime < '").append(toStr).append("' ");
		if (type != null) {
			sql.append("AND t.sub_pkt_type = '").append(type).append("' ");
		}
        sql.append("ORDER BY t.message_datetime DESC ");
        if (limit != null) {
            sql.append("LIMIT ").append(limit);
            if (offset != null && offset > 0) {
                sql.append(" OFFSET ").append(offset);
            }
            sql.append(" ");
        }
        sql.append("FORMAT JSON");
		return sql.toString();
	}

    private String buildSelectSqlJsonEachRow(String fromStr, String toStr, String type, Integer limit, Integer offset) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ")
            .append("t.message_date, t.message_time, t.stationary_kavach_id, t.message_sequence, ")
            .append("t.nms_system_id, t.system_version, t.packet_name, t.sender_identifier, ")
            .append("t.receiver_identifier, t.packet_message_length, t.frame_number, t.packet_message_sequence, ")
            .append("t.border_rfid_tag, t.onboard_kavach_identity, t.sub_pkt_type, t.sub_pkt_len_ma, ")
            .append("t.frame_offset, t.dst_loco_sos, t.train_section_type, t.line_number, t.line_name, ")
            .append("t.type_of_signal, t.signal_ov, t.stop_signal, t.current_sig_aspect, t.next_sig_aspect, ")
            .append("t.authority_type, t.approaching_signal_distance, t.authorized_speed, t.ma_wrt_sig, ")
            .append("t.req_shorten_ma, t.new_ma, t.train_length_info_sts, t.trn_len_info_type, ")
            .append("t.ref_frame_num_tlm, t.ref_offset_int_tlm, t.next_stn_comm, t.appr_stn_ilc_ibs_id, ")
            .append("t.mac_code, t.crc ")
            .append("FROM ").append(database).append(".").append(table).append(" AS t ")
            .append("PREWHERE t.message_datetime >= '").append(fromStr).append("' ")
            .append("AND t.message_datetime < '").append(toStr).append("' ");
        if (type != null) {
            sql.append("AND t.sub_pkt_type = '").append(type).append("' ");
        }
        sql.append("ORDER BY t.message_datetime DESC ");
        if (limit != null) {
            sql.append("LIMIT ").append(limit);
            if (offset != null && offset > 0) {
                sql.append(" OFFSET ").append(offset);
            }
            sql.append(" ");
        }
        sql.append("FORMAT JSONEachRow");
        return sql.toString();
    }

    private String buildSelectSqlDistinct(String fromStr, String toStr, String type, Integer limit, Integer offset) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT DISTINCT ")
            .append("t.message_date, t.message_time, t.stationary_kavach_id, t.message_sequence, ")
            .append("t.nms_system_id, t.system_version, t.packet_name, t.sender_identifier, ")
            .append("t.receiver_identifier, t.packet_message_length, t.frame_number, t.packet_message_sequence, ")
            .append("t.border_rfid_tag, t.onboard_kavach_identity, t.sub_pkt_type, t.sub_pkt_len_ma, ")
            .append("t.frame_offset, t.dst_loco_sos, t.train_section_type, t.line_number, t.line_name, ")
            .append("t.type_of_signal, t.signal_ov, t.stop_signal, t.current_sig_aspect, t.next_sig_aspect, ")
            .append("t.authority_type, t.approaching_signal_distance, t.authorized_speed, t.ma_wrt_sig, ")
            .append("t.req_shorten_ma, t.new_ma, t.train_length_info_sts, t.trn_len_info_type, ")
            .append("t.ref_frame_num_tlm, t.ref_offset_int_tlm, t.next_stn_comm, t.appr_stn_ilc_ibs_id, ")
            .append("t.mac_code, t.crc ")
            .append("FROM ").append(database).append(".").append(table).append(" AS t ")
            .append("PREWHERE t.message_datetime >= '").append(fromStr).append("' ")
            .append("AND t.message_datetime < '").append(toStr).append("' ");
        if (type != null) {
            sql.append("AND t.sub_pkt_type = '").append(type).append("' ");
        }
        sql.append("ORDER BY t.message_datetime DESC ");
        if (limit != null) {
            sql.append("LIMIT ").append(limit);
            if (offset != null && offset > 0) {
                sql.append(" OFFSET ").append(offset);
            }
            sql.append(" ");
        }
        sql.append("FORMAT JSON");
        return sql.toString();
    }

    private String buildSelectSqlJsonEachRowDistinct(String fromStr, String toStr, String type, Integer limit, Integer offset) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT DISTINCT ")
            .append("t.message_date, t.message_time, t.stationary_kavach_id, t.message_sequence, ")
            .append("t.nms_system_id, t.system_version, t.packet_name, t.sender_identifier, ")
            .append("t.receiver_identifier, t.packet_message_length, t.frame_number, t.packet_message_sequence, ")
            .append("t.border_rfid_tag, t.onboard_kavach_identity, t.sub_pkt_type, t.sub_pkt_len_ma, ")
            .append("t.frame_offset, t.dst_loco_sos, t.train_section_type, t.line_number, t.line_name, ")
            .append("t.type_of_signal, t.signal_ov, t.stop_signal, t.current_sig_aspect, t.next_sig_aspect, ")
            .append("t.authority_type, t.approaching_signal_distance, t.authorized_speed, t.ma_wrt_sig, ")
            .append("t.req_shorten_ma, t.new_ma, t.train_length_info_sts, t.trn_len_info_type, ")
            .append("t.ref_frame_num_tlm, t.ref_offset_int_tlm, t.next_stn_comm, t.appr_stn_ilc_ibs_id, ")
            .append("t.mac_code, t.crc ")
            .append("FROM ").append(database).append(".").append(table).append(" AS t ")
            .append("PREWHERE t.message_datetime >= '").append(fromStr).append("' ")
            .append("AND t.message_datetime < '").append(toStr).append("' ");
        if (type != null) {
            sql.append("AND t.sub_pkt_type = '").append(type).append("' ");
        }
        sql.append("ORDER BY t.message_datetime DESC ");
        if (limit != null) {
            sql.append("LIMIT ").append(limit);
            if (offset != null && offset > 0) {
                sql.append(" OFFSET ").append(offset);
            }
            sql.append(" ");
        }
        sql.append("FORMAT JSONEachRow");
        return sql.toString();
    }

    private String buildSelectSqlJsonEachRowUnlimited(String fromStr, String toStr, String type) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ")
            .append("t.message_date, t.message_time, t.stationary_kavach_id, t.message_sequence, ")
            .append("t.nms_system_id, t.system_version, t.packet_name, t.sender_identifier, ")
            .append("t.receiver_identifier, t.packet_message_length, t.frame_number, t.packet_message_sequence, ")
            .append("t.border_rfid_tag, t.onboard_kavach_identity, t.sub_pkt_type, t.sub_pkt_len_ma, ")
            .append("t.frame_offset, t.dst_loco_sos, t.train_section_type, t.line_number, t.line_name, ")
            .append("t.type_of_signal, t.signal_ov, t.stop_signal, t.current_sig_aspect, t.next_sig_aspect, ")
            .append("t.authority_type, t.approaching_signal_distance, t.authorized_speed, t.ma_wrt_sig, ")
            .append("t.req_shorten_ma, t.new_ma, t.train_length_info_sts, t.trn_len_info_type, ")
            .append("t.ref_frame_num_tlm, t.ref_offset_int_tlm, t.next_stn_comm, t.appr_stn_ilc_ibs_id, ")
            .append("t.mac_code, t.crc ")
            .append("FROM ").append(database).append(".").append(table).append(" AS t ")
            .append("PREWHERE t.message_datetime >= '").append(fromStr).append("' ")
            .append("AND t.message_datetime < '").append(toStr).append("' ");
        if (type != null) {
            sql.append("AND t.sub_pkt_type = '").append(type).append("' ");
        }
        sql.append("ORDER BY t.message_datetime DESC ");
        sql.append("FORMAT JSONEachRow");
        return sql.toString();
    }

    private String buildOptimizedClickHouseSettings() {
        StringBuilder settings = new StringBuilder();
        // Use a conservative subset of widely supported settings to avoid 400 Bad Request on older CH versions
        settings.append("SET max_memory_usage = ").append(MAX_MEMORY_USAGE).append("; ")
                .append("SET max_memory_usage_for_user = ").append(MAX_MEMORY_FOR_USER).append("; ")
                .append("SET max_bytes_before_external_group_by = ").append(MAX_BYTES_BEFORE_EXTERNAL_GROUP_BY).append("; ")
                .append("SET max_bytes_before_external_sort = ").append(MAX_BYTES_BEFORE_EXTERNAL_SORT).append("; ")
                .append("SET max_threads = ").append(MAX_THREADS).append("; ")
                .append("SET max_execution_time = ").append(MAX_EXECUTION_TIME).append("; ")
                .append("SET max_block_size = ").append(MAX_BLOCK_SIZE).append("; ")
                .append("SET preferred_block_size_bytes = ").append(PREFERRED_BLOCK_SIZE).append("; ");
        return settings.toString();
    }

    public Mono<Map<String, List<Object>>> fetchDistinctPerColumn(LocalDateTime from,
                                                                  LocalDateTime to,
                                                                  String subPktType,
                                                                  Integer perColumnLimit) {
        String type = (subPktType == null || subPktType.isBlank()) ? null : subPktType.trim();
        if (type != null && !type.matches("\\d{4}")) {
            return Mono.error(new IllegalArgumentException("sub_pkt_type must be 4 digits"));
        }

        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String fromStr = from.format(fmt);
        String toStr = to.format(fmt);

        String[] columns = new String[]{
            "message_date","message_time","stationary_kavach_id","message_sequence",
            "nms_system_id","system_version","packet_name","sender_identifier",
            "receiver_identifier","packet_message_length","frame_number","packet_message_sequence",
            "border_rfid_tag","onboard_kavach_identity","sub_pkt_type","sub_pkt_len_ma",
            "frame_offset","dst_loco_sos","train_section_type","line_number","line_name",
            "type_of_signal","signal_ov","stop_signal","current_sig_aspect","next_sig_aspect",
            "authority_type","approaching_signal_distance","authorized_speed","ma_wrt_sig",
            "req_shorten_ma","new_ma","train_length_info_sts","trn_len_info_type",
            "ref_frame_num_tlm","ref_offset_int_tlm","next_stn_comm","appr_stn_ilc_ibs_id",
            "mac_code","crc"
        };

        java.util.Map<String, List<Object>> result = new java.util.LinkedHashMap<>();

        return reactor.core.publisher.Flux.fromArray(columns)
            .concatMap(col -> queryDistinctColumn(fromStr, toStr, type, col, perColumnLimit)
                .map(values -> {
                    result.put(col, values);
                    return 1; }))
            .then(Mono.fromCallable(() -> result));
    }

    public Mono<List<Object>> fetchDistinctAuthorizedSpeed(LocalDateTime from,
                                                           LocalDateTime to,
                                                           String subPktType,
                                                           Integer limit) {
        String type = (subPktType == null || subPktType.isBlank()) ? null : subPktType.trim();
        if (type != null && !type.matches("\\d{4}")) {
            return Mono.error(new IllegalArgumentException("sub_pkt_type must be 4 digits"));
        }

        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String fromStr = from.format(fmt);
        String toStr = to.format(fmt);

        return queryDistinctColumn(fromStr, toStr, type, "message_datetime", limit);
    }

    public Mono<QueryResult> fetchPacketsFull(LocalDateTime from,
                                              LocalDateTime to,
                                              String subPktType) {
        String type = (subPktType == null || subPktType.isBlank()) ? null : subPktType.trim();
        if (type != null && !type.matches("\\d{4}")) {
            return Mono.error(new IllegalArgumentException("sub_pkt_type must be 4 digits"));
        }

        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String fromStr = from.format(fmt);
        String toStr = to.format(fmt);

        String sql = buildSelectSqlFull(fromStr, toStr, type);

        return webClient.post()
                .uri("/")
                .headers(h -> h.setBasicAuth(username, password))
                .contentType(MediaType.TEXT_PLAIN)
                .bodyValue(sql)
                .retrieve()
                .bodyToMono(String.class)
                .timeout(java.time.Duration.ofMinutes(20))
                .map(json -> {
                    try {
                        long startTime = System.currentTimeMillis();
                        JsonNode root = mapper.readTree(json);
                        JsonNode data = root.get("data");
                        JsonNode stats = root.get("statistics");
                        
                        // Memory monitoring
                        Runtime runtime = Runtime.getRuntime();
                        long maxMemory = runtime.maxMemory();
                        long totalMemory = runtime.totalMemory();
                        long freeMemory = runtime.freeMemory();
                        long usedMemory = totalMemory - freeMemory;
                        long availableMemory = maxMemory - usedMemory;
                        
                        List<Map<String, Object>> out = new ArrayList<>();
                        int rowCount = 0;
                        
                        if (data != null && data.isArray()) {
                            for (JsonNode row : data) {
                                // Check memory every 10,000 rows
                                if (rowCount % 10000 == 0 && rowCount > 0) {
                                    long currentUsedMemory = runtime.totalMemory() - runtime.freeMemory();
                                    long currentAvailableMemory = maxMemory - currentUsedMemory;
                                    
                                    // Warn if less than 500MB available
                                    if (currentAvailableMemory < 500 * 1024 * 1024) {
                                        long elapsedMs = System.currentTimeMillis() - startTime;
                                        System.err.println("WARNING: Memory running low! Fetched " + rowCount + " rows in " + 
                                            elapsedMs + "ms. Available memory: " + (currentAvailableMemory / 1024 / 1024) + "MB");
                                    }
                                    
                                    // Check if we're approaching 80% of max memory
                                    double memoryUsagePercent = (double) currentUsedMemory / maxMemory * 100;
                                    if (memoryUsagePercent > 80) {
                                        long elapsedMs = System.currentTimeMillis() - startTime;
                                        throw new RuntimeException("MEMORY WARNING: Approaching memory limit! " +
                                            "Fetched " + rowCount + " rows in " + elapsedMs + "ms. " +
                                            "Memory usage: " + String.format("%.1f", memoryUsagePercent) + "%");
                                    }
                                }
                                
                                Map<String, Object> map = mapper.convertValue(row, HashMap.class);
                                out.add(map);
                                rowCount++;
                            }
                        }
                        
                        long totalElapsedMs = System.currentTimeMillis() - startTime;
                        long finalUsedMemory = runtime.totalMemory() - runtime.freeMemory();
                        double finalMemoryUsagePercent = (double) finalUsedMemory / maxMemory * 100;
                        
                        System.out.println("Query completed: " + rowCount + " rows fetched in " + totalElapsedMs + "ms. " +
                            "Final memory usage: " + String.format("%.1f", finalMemoryUsagePercent) + "%");
                        
                        Double elapsed = null;
                        if (stats != null && stats.has("elapsed")) {
                            elapsed = stats.get("elapsed").asDouble();
                        }
                        return new QueryResult(out, elapsed);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to parse ClickHouse response", e);
                    }
                });
    }

    private Mono<List<Object>> queryDistinctColumn(String fromStr,
                                                   String toStr,
                                                   String type,
                                                   String column,
                                                   Integer limit) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT DISTINCT ").append(column).append(" FROM ")
            .append(database).append('.').append(table).append(" AS t ")
            .append("PREWHERE t.message_datetime >= '").append(fromStr).append("' ")
            .append("AND t.message_datetime < '").append(toStr).append("' ");
        if (type != null) {
            sql.append("AND t.sub_pkt_type = '").append(type).append("' ");
        }
        sql.append("ORDER BY ").append(column).append(" ASC ");
        if (limit != null && limit > 0) {
            sql.append("LIMIT ").append(limit).append(' ');
        }
        sql.append("FORMAT JSON");

        return webClient.post()
            .uri("/")
            .headers(h -> h.setBasicAuth(username, password))
            .contentType(MediaType.TEXT_PLAIN)
            .bodyValue(sql.toString())
            .retrieve()
            .bodyToMono(String.class)
            .map(json -> {
                try {
                    JsonNode root = mapper.readTree(json);
                    JsonNode data = root.get("data");
                    java.util.List<Object> values = new java.util.ArrayList<>();
                    if (data != null && data.isArray()) {
                        for (JsonNode row : data) {
                            JsonNode v = row.get(column);
                            if (v == null || v.isNull()) {
                                values.add(null);
                            } else if (v.isInt() || v.isLong()) {
                                values.add(v.asLong());
                            } else if (v.isFloat() || v.isDouble() || v.isBigDecimal()) {
                                values.add(v.asDouble());
                            } else if (v.isBoolean()) {
                                values.add(v.asBoolean());
                            } else {
                                values.add(v.asText());
                            }
                        }
                    }
                    return values;
                } catch (Exception e) {
                    throw new RuntimeException("Failed to parse DISTINCT column response for " + column, e);
                }
            });
    }

    private String buildSelectSqlFull(String fromStr, String toStr, String type) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ")
            .append("t.message_date, t.message_time, t.stationary_kavach_id, t.message_sequence, ")
            .append("t.nms_system_id, t.system_version, t.packet_name, t.sender_identifier, ")
            .append("t.receiver_identifier, t.packet_message_length, t.frame_number, t.packet_message_sequence, ")
            .append("t.border_rfid_tag, t.onboard_kavach_identity, t.sub_pkt_type, t.sub_pkt_len_ma, ")
            .append("t.frame_offset, t.dst_loco_sos, t.train_section_type, t.line_number, t.line_name, ")
            .append("t.type_of_signal, t.signal_ov, t.stop_signal, t.current_sig_aspect, t.next_sig_aspect, ")
            .append("t.authority_type, t.approaching_signal_distance, t.authorized_speed, t.ma_wrt_sig, ")
            .append("t.req_shorten_ma, t.new_ma, t.train_length_info_sts, t.trn_len_info_type, ")
            .append("t.ref_frame_num_tlm, t.ref_offset_int_tlm, t.next_stn_comm, t.appr_stn_ilc_ibs_id, ")
            .append("t.mac_code, t.crc ")
            .append("FROM ").append(database).append(".").append(table).append(" AS t ")
            .append("PREWHERE t.message_datetime >= '").append(fromStr).append("' ")
            .append("AND t.message_datetime < '").append(toStr).append("' ");
        if (type != null) {
            sql.append("AND t.sub_pkt_type = '").append(type).append("' ");
        }
        sql.append("ORDER BY t.message_datetime DESC ");
        sql.append("FORMAT JSON");
        return sql.toString();
    }

    private String buildSelectSqlNull(String fromStr, String toStr, String type, Integer limit, Integer offset) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ")
            .append("t.message_date, t.message_time, t.stationary_kavach_id, t.message_sequence, ")
            .append("t.nms_system_id, t.system_version, t.packet_name, t.sender_identifier, ")
            .append("t.receiver_identifier, t.packet_message_length, t.frame_number, t.packet_message_sequence, ")
            .append("t.border_rfid_tag, t.onboard_kavach_identity, t.sub_pkt_type, t.sub_pkt_len_ma, ")
            .append("t.frame_offset, t.dst_loco_sos, t.train_section_type, t.line_number, t.line_name, ")
            .append("t.type_of_signal, t.signal_ov, t.stop_signal, t.current_sig_aspect, t.next_sig_aspect, ")
            .append("t.authority_type, t.approaching_signal_distance, t.authorized_speed, t.ma_wrt_sig, ")
            .append("t.req_shorten_ma, t.new_ma, t.train_length_info_sts, t.trn_len_info_type, ")
            .append("t.ref_frame_num_tlm, t.ref_offset_int_tlm, t.next_stn_comm, t.appr_stn_ilc_ibs_id, ")
            .append("t.mac_code, t.crc ")
            .append("FROM ").append(database).append(".").append(table).append(" AS t ")
            .append("PREWHERE t.message_datetime >= '").append(fromStr).append("' ")
            .append("AND t.message_datetime < '").append(toStr).append("' ");
        if (type != null) {
            sql.append("AND t.sub_pkt_type = '").append(type).append("' ");
        }
        sql.append("ORDER BY t.message_datetime DESC ");
        if (limit != null) {
            sql.append("LIMIT ").append(limit);
            if (offset != null && offset > 0) {
                sql.append(" OFFSET ").append(offset);
            }
            sql.append(" ");
        }
        sql.append("FORMAT Null");
        return sql.toString();
    }
}


