package com.example.service;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.streaming.SXSSFRow;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Value;

import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
 

@Service
public class NativeClickHouseExcelExportService {

	private final java.sql.Connection connection;
	@Value("${clickhouse.database}")
	private String configuredDatabase;
	@Value("${clickhouse.table}")
	private String configuredTable;

	public NativeClickHouseExcelExportService(java.sql.Connection connection) {
		this.connection = connection;
	}

	public List<Path> exportToExcel(LocalDateTime from,
	                               LocalDateTime to,
	                               String subPktType,
	                               int maxRowsPerSheet,
	                               int rowWindowInMemory) throws Exception {
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
			.append("FROM ").append(configuredDatabase).append('.').append(configuredTable).append(" AS t ")
			.append("PREWHERE t.message_datetime >= '").append(fromStr).append("' ")
			.append("AND t.message_datetime < '").append(toStr).append("' ");
		if (subPktType != null && !subPktType.isBlank()) {
			sql.append("AND t.sub_pkt_type = '").append(subPktType.trim()).append("' ");
		}
		sql.append("ORDER BY t.message_datetime DESC ");

		// We will use RowBinary for minimal overhead
		String finalSql = sql.toString();

		List<Path> generated = new ArrayList<>();
		SXSSFWorkbook wb = new SXSSFWorkbook(rowWindowInMemory);
        SXSSFSheet[] currentSheet = new SXSSFSheet[] { wb.createSheet("data-1") };
		int sheetIdx = 1;
		int rowIdx = 0;

		// Write header once per sheet
		String[] headers = new String[]{
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

        java.util.function.Supplier<Row> newHeader = () -> {
            Row headerRow = currentSheet[0].createRow(0);
			for (int i = 0; i < headers.length; i++) {
				headerRow.createCell(i).setCellValue(headers[i]);
			}
			return headerRow;
		};

		newHeader.get();
		rowIdx = 1;

		java.sql.Statement stmt = null;
		java.sql.ResultSet rs = null;
		try {
			stmt = connection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
			stmt.setFetchSize(10_000);
			rs = stmt.executeQuery(finalSql);
			final int columnCount = rs.getMetaData().getColumnCount();
			while (rs.next()) {
                if (rowIdx >= maxRowsPerSheet) {
					Path out = Files.createTempFile("export-" + sheetIdx, ".xlsx");
					try (FileOutputStream fos = new FileOutputStream(out.toFile())) {
						wb.write(fos);
					}
					generated.add(out);
					wb.dispose();
					wb = new SXSSFWorkbook(rowWindowInMemory);
					sheetIdx++;
                    currentSheet[0] = wb.createSheet("data-" + sheetIdx);
					newHeader.get();
					rowIdx = 1;
				}

                SXSSFRow row = currentSheet[0].createRow(rowIdx++);
				for (int i = 1; i <= columnCount; i++) {
					Object v = rs.getObject(i);
					if (v == null) {
						row.createCell(i - 1).setBlank();
					} else if (v instanceof Number) {
						row.createCell(i - 1).setCellValue(((Number) v).doubleValue());
					} else if (v instanceof Boolean) {
						row.createCell(i - 1).setCellValue((Boolean) v);
					} else {
						row.createCell(i - 1).setCellValue(String.valueOf(v));
					}
				}
			}
		} finally {
			if (rs != null) try { rs.close(); } catch (Exception ignored) {}
			if (stmt != null) try { stmt.close(); } catch (Exception ignored) {}
			// write remaining workbook
			Path out = Files.createTempFile("export-" + sheetIdx, ".xlsx");
			try (FileOutputStream fos = new FileOutputStream(out.toFile())) {
				wb.write(fos);
			}
			wb.dispose();
			generated.add(out);
		}

		return generated;
	}
}


