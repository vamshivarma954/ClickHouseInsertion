package com.example.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Native ClickHouse HTTP API service for ultra-fast bulk inserts
 * Uses ClickHouse's native JSONEachRow format for maximum performance
 */
@Service
public class NativeClickHouseInsertService {
    
    private final WebClient webClient;
    private final ObjectMapper objectMapper;
    
    @Value("${clickhouse.url}")
    private String clickhouseUrl;
    
    @Value("${clickhouse.username}")
    private String username;
    
    @Value("${clickhouse.password}")
    private String password;
    
    @Value("${clickhouse.database}")
    private String database;
    
    @Value("${clickhouse.table}")
    private String table;
    
    public NativeClickHouseInsertService(@Value("${clickhouse.url}") String url) {
        this.webClient = WebClient.builder().baseUrl(url).build();
        this.objectMapper = new ObjectMapper();
    }
    
    /**
     * Ultra-fast 6000 individual inserts using ClickHouse native HTTP API
     * Sends all 6000 records in a single HTTP request with JSONEachRow format
     */
    public Mono<NativeInsertResult> insert6000Native() {
        System.out.println("=== Starting 6000 Native ClickHouse Inserts ===");
        System.out.println("Using ClickHouse native HTTP API with JSONEachRow format");
        System.out.println("Single HTTP request with all 6000 records");
        System.out.println("=====================================================");
        
        long startTime = System.currentTimeMillis();
        
        // Generate 6000 records
        List<Map<String, Object>> records = generate6000Records();
        
        // Convert to JSONEachRow format
        String jsonEachRowData = convertToJsonEachRow(records);
        
        // ClickHouse native insert query
        String query = "INSERT INTO " + database + "." + table + " FORMAT JSONEachRow";
        
        return webClient.post()
                .uri(uriBuilder -> {
                    uriBuilder.path("/");
                    uriBuilder.queryParam("query", query);
                    if (username != null && !username.isBlank()) {
                        uriBuilder.queryParam("user", username);
                    }
                    if (password != null && !password.isBlank()) {
                        uriBuilder.queryParam("password", password);
                    }
                    return uriBuilder.build();
                })
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(jsonEachRowData)
                .retrieve()
                .onStatus(status -> !status.is2xxSuccessful(), response ->
                    response.bodyToMono(String.class)
                        .defaultIfEmpty("ClickHouse returned non-2xx")
                        .flatMap(body -> Mono.error(new RuntimeException("ClickHouse error: " + body)))
                )
                .bodyToMono(String.class)
                .map(response -> {
                    long endTime = System.currentTimeMillis();
                    long duration = endTime - startTime;
                    
                    System.out.println("\n=== NATIVE CLICKHOUSE INSERT COMPLETED ===");
                    System.out.println("Total Records: 6000");
                    System.out.println("Duration: " + duration + " ms");
                    System.out.println("Rate: " + (6000.0 / (duration / 1000.0)) + " records/second");
                    System.out.println("==========================================");
                    
                    return new NativeInsertResult(6000, 6000, 0, duration);
                })
                .onErrorResume(error -> {
                    System.err.println("Error in native insert: " + error.getMessage());
                    long endTime = System.currentTimeMillis();
                    long duration = endTime - startTime;
                    return Mono.just(new NativeInsertResult(6000, 0, 6000, duration));
                });
    }
    
    /**
     * Generate 6000 unique records for insertion
     */
    private List<Map<String, Object>> generate6000Records() {
        List<Map<String, Object>> records = new ArrayList<>();
        
        for (int i = 1; i <= 6000; i++) {
            Map<String, Object> record = new LinkedHashMap<>();
            
            String timestamp = LocalDateTime.now()
                    .plusSeconds(i)
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            
            String messageDate = LocalDateTime.now()
                    .format(DateTimeFormatter.ofPattern("dd/MM/yy"));
            String messageTime = LocalDateTime.now()
                    .format(DateTimeFormatter.ofPattern("HH:mm:ss"));
            
            // Populate all required fields
            record.put("message_length", 49);
            record.put("message_sequence", i);
            record.put("stationary_kavach_id", 50000 + (i % 1000));
            record.put("nms_system_id", 1);
            record.put("system_version", 2);
            record.put("message_date", messageDate);
            record.put("message_time", messageTime);
            record.put("crc", "AFCE0000");
            record.put("specific_protocol", "F0");
            record.put("packet_name", "0105");
            record.put("sender_identifier", 50000 + (i % 1000));
            record.put("receiver_identifier", 50001 + (i % 1000));
            record.put("packet_message_length", 28);
            record.put("frame_number", 4444);
            record.put("packet_message_sequence", 2689);
            record.put("border_rfid_tag", 300);
            record.put("ref_profile_id", 0);
            record.put("onboard_kavach_identity", 20000);
            record.put("sub_pkt_type", String.format("%04d", i % 10000));
            record.put("sub_pkt_len_ma", 125);
            record.put("frame_offset", "0000");
            record.put("dst_loco_sos", "01");
            record.put("train_section_type", "00000");
            record.put("line_number", "0000");
            record.put("line_name", "100100");
            record.put("type_of_signal", "0");
            record.put("signal_ov", 0);
            record.put("stop_signal", "011110");
            record.put("current_sig_aspect", "010101");
            record.put("next_sig_aspect", "250");
            record.put("approaching_signal_distance", 1);
            record.put("authority_type", "160");
            record.put("authorized_speed", 120);
            record.put("ma_wrt_sig", 0);
            record.put("req_shorten_ma", "0");
            record.put("new_ma", 0);
            record.put("train_length_info_sts", "0");
            record.put("trn_len_info_type", "0");
            record.put("ref_frame_num_tlm", 0);
            record.put("ref_offset_int_tlm", 0);
            record.put("next_stn_comm", "0");
            record.put("appr_stn_ilc_ibs_id", 0);
            record.put("sub_pkt_type_ssp", "0001");
            record.put("sub_pkt_len_ssp", 28);
            record.put("ssp_count_info", 1);
            record.put("classified_speed_info", "[{\"SpeedB\":0,\"SpeedA\":60}]");
            record.put("sub_pkt_type_gp", "0010");
            record.put("sub_pkt_len_gp", 28);
            record.put("gp_count_info", 1);
            record.put("gradient_info", "[{\"GradientDistance\":1017}]");
            record.put("sub_pkt_type_lc", "0011");
            record.put("sub_pkt_len_lc", 118);
            record.put("lm_count_info", 1);
            record.put("lc_info", "{}");
            record.put("sub_pkt_type_tsp", "0100");
            record.put("sub_pkt_len_tsp", 25);
            record.put("to_count_info", 0);
            record.put("speed_info", "[]");
            record.put("sub_pkt_type_tag", "0101");
            record.put("sub_pkt_len_tag", 25);
            record.put("dist_dup_tag", 0);
            record.put("route_rfid_cnt", 1);
            record.put("rfid_info_list", "[{\"NxtRfidTagId\":303}]");
            record.put("abs_loc_reset", "0");
            record.put("start_dist_to_loc_reset", 0);
            record.put("adj_loc_dir", "0");
            record.put("abs_loc_correction", 0);
            record.put("adj_line_cnt", 0);
            record.put("self_tin", 0);
            record.put("self_tin_list", "[{\"AdjTin\":341}]");
            record.put("sub_pkt_type_tcp", "0110");
            record.put("sub_pkt_len_tcp", 25);
            record.put("track_cond_cnt", 0);
            record.put("track_cond_info", "[]");
            record.put("sub_packet_tsrp", "0111");
            record.put("sub_packet_length_tsrp", 28);
            record.put("tsr_status", "01");
            record.put("tsr_count", 1);
            record.put("tsr_info", "[{\"TSR_ID\":0}]");
            record.put("mac_code", "8C950EFD");
            record.put("created_at", timestamp);
            record.put("message_datetime", timestamp);
            
            records.add(record);
        }
        
        return records;
    }
    
    /**
     * Convert records to ClickHouse JSONEachRow format
     * Each record is a separate JSON line
     */
    private String convertToJsonEachRow(List<Map<String, Object>> records) {
        StringBuilder jsonEachRow = new StringBuilder();
        
        try {
            for (Map<String, Object> record : records) {
                jsonEachRow.append(objectMapper.writeValueAsString(record)).append("\n");
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert records to JSONEachRow format", e);
        }
        
        return jsonEachRow.toString();
    }
    
    public static class NativeInsertResult {
        public final int totalRecords;
        public final int successCount;
        public final int errorCount;
        public final long durationMs;
        public final double recordsPerSecond;
        
        public NativeInsertResult(int totalRecords, int successCount, int errorCount, long durationMs) {
            this.totalRecords = totalRecords;
            this.successCount = successCount;
            this.errorCount = errorCount;
            this.durationMs = durationMs;
            this.recordsPerSecond = (double) successCount / (durationMs / 1000.0);
        }
        
        @Override
        public String toString() {
            return String.format(
                    "Native ClickHouse Insert Results:\n" +
                    "  Total Records: %d\n" +
                    "  Successful: %d\n" +
                    "  Errors: %d\n" +
                    "  Duration: %d ms\n" +
                    "  Rate: %.2f records/second",
                    totalRecords, successCount, errorCount, durationMs, recordsPerSecond
            );
        }
    }
}
