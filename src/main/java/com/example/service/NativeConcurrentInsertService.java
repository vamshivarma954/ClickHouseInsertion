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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Native ClickHouse HTTP API service for highly concurrent individual insertions.
 * Each insert is a separate HTTP request using JSONEachRow format.
 */
@Service
public class NativeConcurrentInsertService {
    /**
     * Schedule 6000 individual ClickHouse inserts per second for a given duration, similar to IoTDB6000RPS.
     * Each insert is a separate HTTP request, rate-limited to 6000/sec.
     * This method blocks until all scheduled inserts are complete.
     */
    public void insert6000RequestsPerSecond(int durationSeconds) throws InterruptedException {
        final int TARGET_RPS = 6000;
        final int THREAD_POOL_SIZE = 2000;
        final long NANO_PER_SECOND = 1_000_000_000L;
        final long REQUEST_INTERVAL_NANOS = NANO_PER_SECOND / TARGET_RPS;

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(THREAD_POOL_SIZE);
        AtomicInteger requestCounter = new AtomicInteger(0);
        AtomicInteger successCounter = new AtomicInteger(0);
        AtomicInteger errorCounter = new AtomicInteger(0);
        long startTime = System.nanoTime();

        for (int second = 0; second < durationSeconds; second++) {
            for (int requestInSecond = 0; requestInSecond < TARGET_RPS; requestInSecond++) {
                final int recordIndex = (second * TARGET_RPS) + requestInSecond;
                long delayNanos = (second * NANO_PER_SECOND) + (requestInSecond * REQUEST_INTERVAL_NANOS);

                scheduler.schedule(() -> {
                    insertIndividualRecord(recordIndex + 1, buildTargetPath())
                        .doOnSuccess(res -> successCounter.incrementAndGet())
                        .doOnError(err -> errorCounter.incrementAndGet())
                        .subscribe();
                    requestCounter.incrementAndGet();
                }, delayNanos, TimeUnit.NANOSECONDS);
            }
        }

        scheduler.shutdown();
        boolean completed = scheduler.awaitTermination(durationSeconds + 10, TimeUnit.SECONDS);

        long totalDuration = (System.nanoTime() - startTime) / 1_000_000;
        int totalRequests = requestCounter.get();
        double actualRPS = (double) totalRequests / (totalDuration / 1000.0);

        System.out.printf(
            "Completed %d requests in %d ms. Success: %d, Errors: %d, Actual RPS: %.2f\n",
            totalRequests, totalDuration, successCounter.get(), errorCounter.get(), actualRPS
        );
    }

    // Helper to build the ClickHouse targetPath string (same as used in insert6000IndividualConcurrent)
    private String buildTargetPath() {
        return org.springframework.web.util.UriComponentsBuilder.fromPath("/")
            .queryParam("query", "INSERT INTO " + database + "." + table + " FORMAT JSONEachRow")
            .queryParam("async_insert", "1")
            .queryParam("wait_for_async_insert", "0")
            .queryParam("async_insert_busy_timeout_ms", "500")
            .queryParam("async_insert_max_data_size", "268435456")
            .queryParam("min_insert_block_size_rows", "5000")
            .queryParam("min_insert_block_size_bytes", "1048576")
            .queryParam("max_memory_usage", "8589934592")
            .queryParam("max_bytes_before_external_sort", "268435456")
            .queryParam("max_bytes_before_external_group_by", "268435456")
            .queryParam("preferred_block_size_bytes", "1048576")
            .queryParam("max_block_size", "16384")
            .queryParam("max_threads", "8")
            .build(false)
            .toUriString();
    }

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
     * Run 6000 individual concurrent inserts - each as a separate HTTP request.
     * This simulates real-world scenario where 6000 packets could go to different tables.
     * Each insert is a separate HTTP request to ClickHouse for maximum concurrency.
     */
    public Mono<NativeConcurrentResult> insert6000IndividualConcurrent() {
        // System.out.println("=== Starting 6000 Individual Concurrent Inserts ===");
        // System.out.println("Each insert is a separate HTTP request (simulating different table scenario)");
        // System.out.println("Total individual requests: " + insertCount);
        // System.out.println("Max concurrency: " + maxConcurrency);
        // System.out.println("ClickHouse async_insert: enabled");
        // System.out.println("ClickHouse min_insert_block_size_rows: 100000");
        // System.out.println("ClickHouse min_insert_block_size_bytes: 67108864");
        // System.out.println("ClickHouse max_memory_usage: 2147483648 (2GB)");
        // System.out.println("ClickHouse max_threads: 4");
        // System.out.println("Server Config Applied (in ClickHouse config):");
        // System.out.println("  - max_server_memory_usage_to_ram_ratio: 0.60");
        // System.out.println("  - mark_cache_size: 536870912");
        // System.out.println("  - index_mark_cache_size: 268435456");
        // System.out.println("  - uncompressed_cache_size: 0");
        // System.out.println("  - number_of_free_entries_in_pool_to_execute_optimize_entire_partition: 8");
        // System.out.println("=====================================================");
        
        // Build query URI with valid ClickHouse HTTP API parameters for fast individual inserts
        final String targetPath = UriComponentsBuilder.fromPath("/")
                .queryParam("query", "INSERT INTO " + database + "." + table + " FORMAT JSONEachRow")
                .queryParam("async_insert", "1")
                .queryParam("wait_for_async_insert", "0")
                .queryParam("async_insert_busy_timeout_ms", "500")
                .queryParam("async_insert_max_data_size", "268435456")
                .queryParam("min_insert_block_size_rows", "5000")
                .queryParam("min_insert_block_size_bytes", "1048576")
                .queryParam("max_memory_usage", "8589934592")
                .queryParam("max_bytes_before_external_sort", "268435456")
                .queryParam("max_bytes_before_external_group_by", "268435456")
                .queryParam("preferred_block_size_bytes", "1048576")
                .queryParam("max_block_size", "16384")
                .queryParam("max_threads", "8")
                .build(false)
                .toUriString();

        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        AtomicLong startTime = new AtomicLong(System.currentTimeMillis());

        // Create 6000 individual insert requests - each as a separate HTTP call
        return Flux.range(1, insertCount)
                .flatMap(sequence -> {
                    return insertIndividualRecord(sequence, targetPath)
                            .retry(2) // Retry up to 2 times on failure
                            .doOnSuccess(count -> {
                                successCount.addAndGet(1);
                                int currentSuccess = successCount.get();
                                if (currentSuccess % 100 == 0) {
                                    long elapsed = System.currentTimeMillis() - startTime.get();
                                    double rate = currentSuccess * 1000.0 / elapsed;
                                    // System.out.println("Progress: " + currentSuccess + "/" + insertCount + 
                                                    //  " inserts completed (" + String.format("%.1f", rate) + " inserts/sec)");
                                }
                            })
                            .doOnError(err -> {
                                errorCount.addAndGet(1);
                                System.err.println("Individual insert " + sequence + " failed after retries: " + err.getMessage());
                            })
                            .onErrorResume(e -> Mono.just(0));
                }, maxConcurrency) // Use controlled concurrency for individual requests
                .then(Mono.fromCallable(() -> {
                    long endTime = System.currentTimeMillis();
                    long duration = endTime - startTime.get();
                    
                    // System.out.println("\n=== INDIVIDUAL CONCURRENT INSERTS COMPLETED ===");
                    // System.out.println("Total Individual Requests: " + insertCount);
                    // System.out.println("Successful: " + successCount.get());
                    // System.out.println("Errors: " + errorCount.get());
                    // System.out.println("Duration: " + duration + " ms");
                    // System.out.println("Rate: " + String.format("%.2f", successCount.get() * 1000.0 / duration) + " individual inserts/second");
                    // System.out.println("Success Rate: " + String.format("%.2f", (successCount.get() * 100.0 / insertCount)) + "%");
                    // System.out.println("Average Time per Insert: " + String.format("%.2f", (double) duration / successCount.get()) + " ms");
                    // System.out.println("ClickHouse Configuration Applied:");
                    // System.out.println("  - async_insert: enabled");
                    // System.out.println("  - max_memory_usage: 2GB");
                    // System.out.println("  - max_threads: 4");
                    // System.out.println("  - max_server_memory_usage_to_ram_ratio: 0.60");
                    // System.out.println("  - mark_cache_size: 512MB");
                    // System.out.println("  - index_mark_cache_size: 256MB");
                    // System.out.println("===============================================");
                    
                    return new NativeConcurrentResult(
                            insertCount,
                            successCount.get(),
                            errorCount.get(),
                            duration
                    );
                }));
    }

    /**
     * Insert a single individual record (simulating packet to different table scenario).
     */
    private Mono<Integer> insertIndividualRecord(int sequence, String targetPath) {
        try {
            Map<String, Object> record = createRecordData(sequence);
            String jsonData = objectMapper.writeValueAsString(record);
            
            return webClient.post()
                    .uri(targetPath)
                    .headers(h -> h.setBasicAuth(username, password))
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue(jsonData)
                    .retrieve()
                    .toBodilessEntity()
                    .then(Mono.just(1)); // Return 1 for successful individual insert
                    
        } catch (Exception e) {
            return Mono.error(new RuntimeException("Individual insert failed for record " + sequence, e));
        }
    }

    /**
     * Create optimized record data for a given sequence number.
     * This method is optimized for performance with minimal object creation.
     */
    public Map<String, Object> createRecordData(int sequence) {
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

