package com.example.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class JdbcClickHouseQueryService {

	private final Connection connection;

	@Value("${clickhouse.database}")
	private String database;

	@Value("${clickhouse.table}")
	private String table;

	public JdbcClickHouseQueryService(Connection connection) {
		this.connection = connection;
	}

	public List<Map<String, Object>> fetchPackets(LocalDateTime from,
	                                             LocalDateTime to,
	                                             String subPktType,
	                                             Integer limit,
	                                             Integer offset) throws Exception {
		String type = (subPktType == null || subPktType.isBlank()) ? null : subPktType.trim();
		if (type != null && !type.matches("\\d{4}")) {
			throw new IllegalArgumentException("sub_pkt_type must be 4 digits");
		}
		DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
		String fromStr = from.format(fmt);
		String toStr = to.format(fmt);

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
			.append("FROM ").append(database).append('.').append(table).append(" AS t ")
			.append("PREWHERE t.message_datetime >= ? AND t.message_datetime < ? ");
		if (type != null) {
			sql.append("AND t.sub_pkt_type = ? ");
		}
		sql.append("ORDER BY t.message_datetime DESC ");
		if (limit != null) {
			sql.append("LIMIT ? ");
			if (offset != null && offset > 0) {
				sql.append("OFFSET ? ");
			}
		}

		try (PreparedStatement ps = connection.prepareStatement(sql.toString(), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
			int idx = 1;
			ps.setString(idx++, fromStr);
			ps.setString(idx++, toStr);
			if (type != null) ps.setString(idx++, type);
			if (limit != null) {
				ps.setInt(idx++, limit);
				if (offset != null && offset > 0) ps.setInt(idx++, offset);
			}
			ps.setFetchSize(10_000);
			try (ResultSet rs = ps.executeQuery()) {
				return readRows(rs);
			}
		}
	}

	public long fetchPacketsCount(LocalDateTime from, LocalDateTime to, String subPktType) throws Exception {
		String type = (subPktType == null || subPktType.isBlank()) ? null : subPktType.trim();
		if (type != null && !type.matches("\\d{4}")) {
			throw new IllegalArgumentException("sub_pkt_type must be 4 digits");
		}
		DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
		String fromStr = from.format(fmt);
		String toStr = to.format(fmt);
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT count() AS cnt FROM ")
			.append(database).append('.').append(table).append(" AS t ")
			.append("PREWHERE t.message_datetime >= ? AND t.message_datetime < ? ");
		if (type != null) sql.append("AND t.sub_pkt_type = ? ");
		try (PreparedStatement ps = connection.prepareStatement(sql.toString())) {
			int idx = 1;
			ps.setString(idx++, fromStr);
			ps.setString(idx++, toStr);
			if (type != null) ps.setString(idx++, type);
			try (ResultSet rs = ps.executeQuery()) {
				if (rs.next()) return rs.getLong(1);
			}
		}
		throw new RuntimeException("COUNT failed");
	}

	public List<Map<String, Object>> fetchDistinct(LocalDateTime from,
	                                              LocalDateTime to,
	                                              String subPktType,
	                                              Integer limit,
	                                              Integer offset) throws Exception {
		String type = (subPktType == null || subPktType.isBlank()) ? null : subPktType.trim();
		if (type != null && !type.matches("\\d{4}")) {
			throw new IllegalArgumentException("sub_pkt_type must be 4 digits");
		}
		DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
		String fromStr = from.format(fmt);
		String toStr = to.format(fmt);
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
			.append("FROM ").append(database).append('.').append(table).append(" AS t ")
			.append("PREWHERE t.message_datetime >= ? AND t.message_datetime < ? ");
		if (type != null) sql.append("AND t.sub_pkt_type = ? ");
		sql.append("ORDER BY t.message_datetime DESC ");
		if (limit != null) {
			sql.append("LIMIT ? ");
			if (offset != null && offset > 0) sql.append("OFFSET ? ");
		}
		try (PreparedStatement ps = connection.prepareStatement(sql.toString(), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
			int idx = 1;
			ps.setString(idx++, fromStr);
			ps.setString(idx++, toStr);
			if (type != null) ps.setString(idx++, type);
			if (limit != null) {
				ps.setInt(idx++, limit);
				if (offset != null && offset > 0) ps.setInt(idx++, offset);
			}
			ps.setFetchSize(10_000);
			try (ResultSet rs = ps.executeQuery()) {
				return readRows(rs);
			}
		}
	}

	public Map<String, List<Object>> fetchDistinctPerColumn(LocalDateTime from,
	                                                      LocalDateTime to,
	                                                      String subPktType,
	                                                      Integer perColumnLimit) throws Exception {
		String type = (subPktType == null || subPktType.isBlank()) ? null : subPktType.trim();
		if (type != null && !type.matches("\\d{4}")) {
			throw new IllegalArgumentException("sub_pkt_type must be 4 digits");
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
		Map<String, List<Object>> out = new java.util.LinkedHashMap<>();
		for (String col : columns) {
			StringBuilder sql = new StringBuilder();
			sql.append("SELECT DISTINCT ").append(col)
				.append(" FROM ").append(database).append('.').append(table).append(" AS t ")
				.append("PREWHERE t.message_datetime >= ? AND t.message_datetime < ? ");
			if (type != null) sql.append("AND t.sub_pkt_type = ? ");
			sql.append("ORDER BY ").append(col).append(" ASC ");
			if (perColumnLimit != null && perColumnLimit > 0) sql.append("LIMIT ? ");
			try (PreparedStatement ps = connection.prepareStatement(sql.toString())) {
				int idx = 1;
				ps.setString(idx++, fromStr);
				ps.setString(idx++, toStr);
				if (type != null) ps.setString(idx++, type);
				if (perColumnLimit != null && perColumnLimit > 0) ps.setInt(idx++, perColumnLimit);
				try (ResultSet rs = ps.executeQuery()) {
					List<Object> values = new ArrayList<>();
					while (rs.next()) {
						Object v = rs.getObject(1);
						values.add(v);
					}
					out.put(col, values);
				}
			}
		}
		return out;
	}

	public List<Object> fetchDistinctAuthorizedSpeed(LocalDateTime from,
	                                                LocalDateTime to,
	                                                String subPktType,
	                                                Integer limit) throws Exception {
		String type = (subPktType == null || subPktType.isBlank()) ? null : subPktType.trim();
		if (type != null && !type.matches("\\d{4}")) {
			throw new IllegalArgumentException("sub_pkt_type must be 4 digits");
		}
		DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
		String fromStr = from.format(fmt);
		String toStr = to.format(fmt);
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT DISTINCT authorized_speed FROM ")
			.append(database).append('.').append(table).append(" AS t ")
			.append("PREWHERE t.message_datetime >= ? AND t.message_datetime < ? ");
		if (type != null) sql.append("AND t.sub_pkt_type = ? ");
		sql.append("ORDER BY authorized_speed ASC ");
		if (limit != null && limit > 0) sql.append("LIMIT ? ");
		try (PreparedStatement ps = connection.prepareStatement(sql.toString())) {
			int idx = 1;
			ps.setString(idx++, fromStr);
			ps.setString(idx++, toStr);
			if (type != null) ps.setString(idx++, type);
			if (limit != null && limit > 0) ps.setInt(idx++, limit);
			try (ResultSet rs = ps.executeQuery()) {
				List<Object> values = new ArrayList<>();
				while (rs.next()) values.add(rs.getObject(1));
				return values;
			}
		}
	}

	public List<Map<String, Object>> fetchPacketsFull(LocalDateTime from,
	                                                 LocalDateTime to,
	                                                 String subPktType) throws Exception {
		return fetchPackets(from, to, subPktType, null, null);
	}

	public long streamTiming(LocalDateTime from,
	                         LocalDateTime to,
	                         String subPktType,
	                         Integer limit,
	                         Integer offset) throws Exception {
		// Iterate rows and count
		List<Map<String, Object>> rows = fetchPackets(from, to, subPktType, limit, offset);
		return rows.size();
	}

	public long streamTimingUnlimited(LocalDateTime from,
	                                  LocalDateTime to,
	                                  String subPktType) throws Exception {
		// Iterate rows and count without limit
		List<Map<String, Object>> rows = fetchPackets(from, to, subPktType, null, null);
		return rows.size();
	}

	private List<Map<String, Object>> readRows(ResultSet rs) throws Exception {
		List<Map<String, Object>> out = new ArrayList<>();
		ResultSetMetaData md = rs.getMetaData();
		int cols = md.getColumnCount();
		while (rs.next()) {
			Map<String, Object> row = new HashMap<>();
			for (int i = 1; i <= cols; i++) {
				row.put(md.getColumnLabel(i), rs.getObject(i));
			}
			out.add(row);
		}
		return out;
	}
}


