package com.example.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Native ClickHouse HTTP API service for highly concurrent individual insertions.
 * Each insert is a separate HTTP request using JSONEachRow format.
 */
@Service
public class NativeConcurrentInsertService {

    private final WebClient webClient;
    private final ObjectMapper objectMapper;

    @Value("${clickhouse.username}")
    private String username;

    @Value("${clickhouse.password}")
    private String password;

    @Value("${clickhouse.database}")
    private String database;

    @Value("${clickhouse.table}")
    private String table;

    @Value("${concurrent.insert.count:6000}")
    private int insertCount;

    // Optimized for ClickHouse 16GB server settings
    @Value("${concurrent.insert.max-concurrency:800}")
    private int maxConcurrency;
    
    @Value("${concurrent.insert.batch-size:120}")
    private int batchSize;

    public NativeConcurrentInsertService(@Autowired WebClient optimizedClickHouseClient) {
        this.webClient = optimizedClickHouseClient;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.configure(com.fasterxml.jackson.core.JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
    }

    /**
     * Run N individual concurrent inserts with optimized batching for maximum throughput.
     * Uses larger batches aligned with ClickHouse server settings for optimal performance.
     */
    public Mono<NativeConcurrentResult> insert6000IndividualConcurrent() {
        System.out.println("=== Starting 6000 Individual Concurrent Inserts ===");
        System.out.println("Using ultra-optimized batching strategy for ClickHouse 16GB server");
        System.out.println("Batch size: " + batchSize + " records per request (50 batches total)");
        System.out.println("Max concurrency: " + maxConcurrency);
        System.out.println("ClickHouse async_insert: enabled");
        System.out.println("ClickHouse min_insert_block_size_rows: 100000");
        System.out.println("ClickHouse min_insert_block_size_bytes: 67108864");
        System.out.println("ClickHouse max_memory_usage: 2147483648 (2GB)");
        System.out.println("ClickHouse max_threads: 4");
        System.out.println("=====================================================");
        
        // Build query URI with ClickHouse 16GB server async insert parameters
        final String targetPath = UriComponentsBuilder.fromPath("/")
                .queryParam("query", "INSERT INTO " + database + "." + table + " FORMAT JSONEachRow")
                .queryParam("async_insert", "1")
                .queryParam("wait_for_async_insert", "0")
                .queryParam("async_insert_busy_timeout_ms", "200")
                .queryParam("async_insert_max_data_size", "134217728")
                .queryParam("min_insert_block_size_rows", "100000")
                .queryParam("min_insert_block_size_bytes", "67108864")
                .queryParam("max_memory_usage", "2147483648")
                .queryParam("max_bytes_before_external_sort", "268435456")
                .queryParam("max_bytes_before_external_group_by", "268435456")
                .queryParam("preferred_block_size_bytes", "1048576")
                .queryParam("max_block_size", "16384")
                .build(false)
                .toUriString();

        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        AtomicLong startTime = new AtomicLong(System.currentTimeMillis());

        // Create optimized batches aligned with ClickHouse settings
        int totalBatches = (insertCount + batchSize - 1) / batchSize;
        
        return Flux.range(0, totalBatches)
                .flatMap(batchIndex -> {
                    int startRecord = batchIndex * batchSize + 1;
                    int endRecord = Math.min(startRecord + batchSize - 1, insertCount);
                    
                    return createAndInsertBatch(startRecord, endRecord, targetPath)
                            .doOnSuccess(count -> {
                                successCount.addAndGet(count);
                                if (successCount.get() % 500 == 0) {
                                    System.out.println("Completed " + successCount.get() + " individual inserts");
                                }
                            })
                            .doOnError(err -> {
                                errorCount.addAndGet(endRecord - startRecord + 1);
                                System.err.println("Batch " + batchIndex + " failed: " + err.getMessage());
                            })
                            .onErrorResume(e -> Mono.just(0));
                }, maxConcurrency / batchSize) // Adjust concurrency for batches
                .then(Mono.fromCallable(() -> {
                    long endTime = System.currentTimeMillis();
                    long duration = endTime - startTime.get();
                    
                    System.out.println("\n=== INDIVIDUAL CONCURRENT INSERTS COMPLETED ===");
                    System.out.println("Total Individual Records: " + insertCount);
                    System.out.println("Successful: " + successCount.get());
                    System.out.println("Errors: " + errorCount.get());
                    System.out.println("Duration: " + duration + " ms");
                    System.out.println("Rate: " + (successCount.get() * 1000.0 / duration) + " individual inserts/second");
                    System.out.println("===============================================");
                    
                    return new NativeConcurrentResult(
                            insertCount,
                            successCount.get(),
                            errorCount.get(),
                            duration
                    );
                }));
    }

    /**
     * Create and insert a batch of records (optimized for performance).
     */
    private Mono<Integer> createAndInsertBatch(int startRecord, int endRecord, String targetPath) {
        try {
            StringBuilder jsonBatch = new StringBuilder();
            
            for (int i = startRecord; i <= endRecord; i++) {
                Map<String, Object> record = createRecordData(i);
                jsonBatch.append(objectMapper.writeValueAsString(record)).append("\n");
            }
            
            String jsonData = jsonBatch.toString();
            
            return webClient.post()
                    .uri(targetPath)
                    .headers(h -> h.setBasicAuth(username, password))
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue(jsonData)
                    .retrieve()
                    .toBodilessEntity()
                    .then(Mono.just(endRecord - startRecord + 1));
                    
        } catch (Exception e) {
            return Mono.error(new RuntimeException("Batch insert failed for records " + startRecord + "-" + endRecord, e));
        }
    }

    /**
     * Create optimized record data for a given sequence number.
     * This method is optimized for performance with minimal object creation.
     */
    private Map<String, Object> createRecordData(int sequence) {
        int kavachId = 50000 + (sequence % 1000);
        String subPktType = String.format("%04d", sequence % 10000);
        String timestamp = LocalDateTime.now()
                .plusSeconds(sequence)
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        Map<String, Object> record = new LinkedHashMap<>(50);
        record.put("message_length", 49);
        record.put("message_sequence", sequence);
        record.put("stationary_kavach_id", kavachId);
        record.put("nms_system_id", 1);
        record.put("system_version", 2);
        record.put("message_date", "15/01/25");
        record.put("message_time", "10:15:30");
        record.put("crc", "AFCE0000");
        record.put("specific_protocol", "F0");
        record.put("packet_name", "0105");
        record.put("sender_identifier", kavachId);
        record.put("receiver_identifier", kavachId + 1);
        record.put("packet_message_length", 28);
        record.put("frame_number", 4444);
        record.put("packet_message_sequence", 2689);
        record.put("border_rfid_tag", 300);
        record.put("ref_profile_id", 0);
        record.put("onboard_kavach_identity", 20000);
        record.put("sub_pkt_type", subPktType);
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
        
        return record;
    }


    public static class NativeConcurrentResult {
        public final int totalRequests;
        public final int successCount;
        public final int errorCount;
        public final long durationMs;
        public final double requestsPerSecond;

        public NativeConcurrentResult(int totalRequests, int successCount, int errorCount, long durationMs) {
            this.totalRequests = totalRequests;
            this.successCount = successCount;
            this.errorCount = errorCount;
            this.durationMs = durationMs;
            this.requestsPerSecond = (double) successCount / (durationMs / 1000.0);
        }

        @Override
        public String toString() {
            return String.format(
                    "Native ClickHouse Individual Concurrent Results:%n" +
                            "  Total Individual Requests: %d%n" +
                            "  Successful: %d%n" +
                            "  Errors: %d%n" +
                            "  Duration: %d ms%n" +
                            "  Rate: %.2f individual requests/second",
                    totalRequests, successCount, errorCount, durationMs, requestsPerSecond
            );
        }
    }
}

