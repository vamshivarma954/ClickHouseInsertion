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


