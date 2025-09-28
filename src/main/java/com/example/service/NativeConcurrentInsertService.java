package com.example.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;
import io.netty.channel.ChannelOption;

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

    // Default tuned to 512; increase up to 1024 if your box/network can handle it
    @Value("${concurrent.insert.max-concurrency:512}")
    private int maxConcurrency;

    public NativeConcurrentInsertService(@Autowired WebClient optimizedClickHouseClient) {
        this.webClient = optimizedClickHouseClient;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.configure(com.fasterxml.jackson.core.JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
    }

    /**
     * Run N individual concurrent inserts.
     */
    public Mono<NativeConcurrentResult> insert6000IndividualConcurrent() {
        // Build query URI once
        final String targetPath = UriComponentsBuilder.fromPath("/")
                .queryParam("query", "INSERT INTO " + database + "." + table + " FORMAT JSONEachRow")
                .build(false)
                .toUriString();

        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        AtomicLong startTime = new AtomicLong(System.currentTimeMillis());

        return Flux.range(1, insertCount)
                .flatMap(i ->
                                createAndInsertIndividualRecord(i, targetPath)
                                        .doOnSuccess(ignored -> successCount.incrementAndGet())
                                        .doOnError(err -> errorCount.incrementAndGet())
                                        .onErrorResume(e -> Mono.empty()),
                        maxConcurrency)
                .then(Mono.fromCallable(() -> {
                    long endTime = System.currentTimeMillis();
                    long duration = endTime - startTime.get();
                    return new NativeConcurrentResult(
                            insertCount,
                            successCount.get(),
                            errorCount.get(),
                            duration
                    );
                }));
    }

    /**
     * Create one record and insert it (non-blocking).
     */
    private Mono<Void> createAndInsertIndividualRecord(int sequence, String targetPath) {
        try {
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

            String jsonLine = objectMapper.writeValueAsString(record) + "\n";

            return webClient.post()
                    .uri(targetPath) // reuse prebuilt URI with encoded query
                    .headers(h -> h.setBasicAuth(username, password)) // Basic Auth
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue(jsonLine)
                    .retrieve()
                    .toBodilessEntity()
                    .then();

        } catch (Exception e) {
            return Mono.error(new RuntimeException("Insert failed for sequence " + sequence, e));
        }
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

