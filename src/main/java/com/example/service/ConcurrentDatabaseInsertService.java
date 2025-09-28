package com.example.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class ConcurrentDatabaseInsertService {
    
    private final JdbcTemplate jdbcTemplate;
    
    @Value("${concurrent.insert.count:6000}")
    private int insertCount;
    
    @Value("${concurrent.insert.max-concurrency:200}")
    private int maxConcurrency;
    
    public ConcurrentDatabaseInsertService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }
    
    /**
     * Single API call that performs 6000 concurrent individual inserts
     * Each insert is a separate database operation, not a batch
     */
    public Mono<ConcurrentInsertResult> execute6000ConcurrentInserts() {
        System.out.println("Starting 6000 concurrent individual database inserts...");
        
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        AtomicLong startTime = new AtomicLong(System.currentTimeMillis());
        
        return Flux.range(1, insertCount)
                .flatMap(i -> createAndInsertRecord(i)
                        .doOnSuccess(result -> {
                            successCount.incrementAndGet();
                            if (successCount.get() % 100 == 0) {
                                System.out.println("Completed " + successCount.get() + " inserts");
                            }
                        })
                        .doOnError(error -> {
                            errorCount.incrementAndGet();
                            System.err.println("Error in insert " + i + ": " + error.getMessage());
                        })
                        .onErrorResume(e -> Mono.empty()) // Continue on error
                        .subscribeOn(Schedulers.boundedElastic()),
                        maxConcurrency) // Limit concurrent operations
                .then(Mono.fromCallable(() -> {
                    long endTime = System.currentTimeMillis();
                    long duration = endTime - startTime.get();
                    
                    return new ConcurrentInsertResult(
                            insertCount,
                            successCount.get(),
                            errorCount.get(),
                            duration
                    );
                }));
    }
    
    private Mono<Void> createAndInsertRecord(int sequence) {
        return Mono.fromRunnable(() -> {
            try {
                // Create unique data for each insert
                String timestamp = LocalDateTime.now()
                        .plusSeconds(sequence)
                        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                
                String messageDate = LocalDateTime.now()
                        .format(DateTimeFormatter.ofPattern("dd/MM/yy"));
                String messageTime = LocalDateTime.now()
                        .format(DateTimeFormatter.ofPattern("HH:mm:ss"));
                
                // Individual INSERT statement for each record
                String sql = """
                    INSERT INTO kavach.nms_event_adjacent_kavach_train_rri
                    (
                        message_length, message_sequence, stationary_kavach_id, nms_system_id, system_version,
                        message_date, message_time, crc, specific_protocol, packet_name,
                        sender_identifier, receiver_identifier, packet_message_length, frame_number, packet_message_sequence,
                        border_rfid_tag, ref_profile_id, onboard_kavach_identity, sub_pkt_type, sub_pkt_len_ma,
                        frame_offset, dst_loco_sos, train_section_type, line_number, line_name,
                        type_of_signal, signal_ov, stop_signal, current_sig_aspect, next_sig_aspect,
                        approaching_signal_distance, authority_type, authorized_speed, ma_wrt_sig, req_shorten_ma,
                        new_ma, train_length_info_sts, trn_len_info_type, ref_frame_num_tlm, ref_offset_int_tlm,
                        next_stn_comm, appr_stn_ilc_ibs_id, sub_pkt_type_ssp, sub_pkt_len_ssp, ssp_count_info,
                        classified_speed_info, sub_pkt_type_gp, sub_pkt_len_gp, gp_count_info, gradient_info,
                        sub_pkt_type_lc, sub_pkt_len_lc, lm_count_info, lc_info, sub_pkt_type_tsp,
                        sub_pkt_len_tsp, to_count_info, speed_info, sub_pkt_type_tag, sub_pkt_len_tag,
                        dist_dup_tag, route_rfid_cnt, rfid_info_list, abs_loc_reset, start_dist_to_loc_reset,
                        adj_loc_dir, abs_loc_correction, adj_line_cnt, self_tin, self_tin_list,
                        sub_pkt_type_tcp, sub_pkt_len_tcp, track_cond_cnt, track_cond_info, sub_packet_tsrp,
                        sub_packet_length_tsrp, tsr_status, tsr_count, tsr_info, mac_code,
                        created_at, message_datetime
                    )
                    VALUES (?,?,?,?, ?,?,?,?, ?,?,
                            ?,?,?,?, ?,?,?,?, ?,?,
                            ?,?,?,?, ?,?,
                            ?,?,?,?, ?,?,
                            ?,?,?,?, ?,
                            ?,?,?,?, ?,?,
                            ?,?,?,?, ?,?,
                            ?,?,?,?, ?,
                            ?,?,?,?, ?,?,
                            ?,?,?,?, ?,?,
                            ?,?,?,?, ?,
                            ?,?,?,?, ?,?,
                            ?,?)
                    """;
                
                // Execute individual insert
                jdbcTemplate.update(sql,
                    49, sequence, 50000 + (sequence % 1000), 1, 2,
                    messageDate, messageTime, "AFCE0000", "F0", "0105",
                    50000 + (sequence % 1000), 50001 + (sequence % 1000), 28, 4444, 2689,
                    300, 0, 20000, String.format("%04d", sequence % 10000), 125,
                    "0000", "01", "00000", "0000", "100100",
                    "0", 0, "011110", "010101", "250",
                    1, "160", 120, 0, "0",
                    0, "0", "0", 0, 0,
                    "0", 0, "0001", 28, 1,
                    "[{\"SpeedB\":0,\"SpeedA\":60}]", "0010", 28, 1, "[{\"GradientDistance\":1017}]",
                    "0011", 118, 1, "{}", "0100",
                    25, 0, "[]", "0101", 25,
                    0, 1, "[{\"NxtRfidTagId\":303}]", "0", 0,
                    "0", 0, 0, 0, "[{\"AdjTin\":341}]",
                    "0110", 25, 0, "[]", "0111",
                    28, "01", 1, "[{\"TSR_ID\":0}]", "8C950EFD",
                    timestamp, timestamp
                );
                
            } catch (Exception e) {
                throw new RuntimeException("Database insert failed for sequence " + sequence, e);
            }
        });
    }
    
    public static class ConcurrentInsertResult {
        public final int totalRequests;
        public final int successCount;
        public final int errorCount;
        public final long durationMs;
        public final double requestsPerSecond;
        
        public ConcurrentInsertResult(int totalRequests, int successCount, int errorCount, long durationMs) {
            this.totalRequests = totalRequests;
            this.successCount = successCount;
            this.errorCount = errorCount;
            this.durationMs = durationMs;
            this.requestsPerSecond = (double) successCount / (durationMs / 1000.0);
        }
        
        @Override
        public String toString() {
            return String.format(
                    "Concurrent Insert Results:\n" +
                    "  Total Requests: %d\n" +
                    "  Successful: %d\n" +
                    "  Errors: %d\n" +
                    "  Duration: %d ms\n" +
                    "  Rate: %.2f inserts/second",
                    totalRequests, successCount, errorCount, durationMs, requestsPerSecond
            );
        }
    }
}
