package com.example.service;

import org.springframework.stereotype.Service;
import com.example.api.InsertOneRequest;
import reactor.core.publisher.Mono;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;

@Service
public class ClickHouseInsertService {
  private final ObjectMapper mapper = new ObjectMapper();
  private final WebClient ch;

  @Value("${clickhouse.database}")
  private String database;
  @Value("${clickhouse.table}")
  private String table;
  @Value("${clickhouse.username}")
  private String username;
  @Value("${clickhouse.password}")
  private String password;

  public ClickHouseInsertService(@Value("${clickhouse.url}") String url) {
    this.ch = WebClient.builder().baseUrl(url).build();
  }

  public Mono<Void> insertOne(InsertOneRequest req) {
    // Accept "yyyy-MM-dd HH:mm:ss" or "yyyy-MM-dd'T'HH:mm:ss"
    final String raw = req.messageDatetime();
    if (raw == null || raw.isBlank()) return Mono.error(new IllegalArgumentException("messageDatetime is required"));

    final String normalized = raw.replace('T', ' ');
    LocalDateTime dt;
    try {
      dt = LocalDateTime.parse(normalized, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    } catch (Exception e) {
      return Mono.error(new IllegalArgumentException("messageDatetime must be yyyy-MM-dd HH:mm:ss"));
    }
    String messageDate = dt.format(DateTimeFormatter.ofPattern("dd/MM/yy"));
    String messageTime = dt.format(DateTimeFormatter.ofPattern("HH:mm:ss"));

    Map<String, Object> row = new LinkedHashMap<>();
    row.put("message_length", 49);
    row.put("message_sequence", req.messageSequence());
    row.put("stationary_kavach_id", req.stationaryKavachId());
    row.put("nms_system_id", 1);
    row.put("system_version", 2);
    row.put("message_date", messageDate);
    row.put("message_time", messageTime);
    row.put("crc", "AFCE0000");
    row.put("specific_protocol", "F0");
    row.put("packet_name", "0105");
    row.put("sender_identifier", req.stationaryKavachId());
    row.put("receiver_identifier", req.stationaryKavachId() + 1);
    row.put("packet_message_length", 28);
    row.put("frame_number", 4444);
    row.put("packet_message_sequence", 2689);
    row.put("border_rfid_tag", 300);
    row.put("ref_profile_id", 0);
    row.put("onboard_kavach_identity", 20000);
    row.put("sub_pkt_type", req.subPktType());
    row.put("sub_pkt_len_ma", 125);
    row.put("frame_offset", "0000");
    row.put("dst_loco_sos", "01");
    row.put("train_section_type", "00000");
    row.put("line_number", "0000");
    row.put("line_name", "100100");
    row.put("type_of_signal", 0);
    row.put("signal_ov", 0);
    row.put("stop_signal", "011110");
    row.put("current_sig_aspect", "010101");
    row.put("next_sig_aspect", 250);
    row.put("approaching_signal_distance", "01");
    row.put("authority_type", 160);
    row.put("authorized_speed", 120);
    row.put("ma_wrt_sig", 0);
    row.put("req_shorten_ma", 0);
    row.put("new_ma", 0);
    row.put("ref_offset_int_tlm", 0);
    row.put("next_stn_comm", "0");
    row.put("appr_stn_ilc_ibs_id", 0);
    row.put("sub_pkt_type_ssp", "0001");
    row.put("sub_pkt_len_ssp", 28);
    row.put("ssp_count_info", 1);
    row.put("classified_speed_info", "[{\"SpeedB\":0,\"SpeedA\":60}]");
    row.put("sub_pkt_type_gp", "0010");
    row.put("sub_pkt_len_gp", 28);
    row.put("gp_count_info", 1);
    row.put("gradient_info", "[{\"GradientDistance\":1017}]");
    row.put("sub_pkt_type_lc", "0011");
    row.put("sub_pkt_len_lc", 118);
    row.put("lm_count_info", 1);
    row.put("sub_pkt_type_tsp", "0100");
    row.put("sub_pkt_len_tsp", 25);
    row.put("to_count_info", 0);
    row.put("sub_pkt_type_tag", "0101");
    row.put("sub_pkt_len_tag", 25);
    row.put("dist_dup_tag", 0);
    row.put("route_rfid_cnt", 1);
    row.put("rfid_info_list", "[{\"NxtRfidTagId\":303}]");
    row.put("abs_loc_correction", 0);
    row.put("self_tin", 0);
    row.put("self_tin_list", "[{\"AdjTin\":341}]");
    row.put("sub_pkt_type_tcp", "0110");
    row.put("sub_pkt_len_tcp", 25);
    row.put("track_cond_cnt", 0);
    row.put("sub_packet_tsrp", "0111");
    row.put("sub_packet_length_tsrp", 28);
    row.put("tsr_status", "01");
    row.put("tsr_count", 1);
    row.put("tsr_info", "[{\"TSR_ID\":0}]");
    row.put("mac_code", "8C950EFD");
    row.put("created_at", normalized);
    row.put("message_datetime", normalized);

    String oneJsonLine;
    try { oneJsonLine = mapper.writeValueAsString(row) + "\n"; }
    catch (Exception e) { return Mono.error(e); }

    final String query = "INSERT INTO " + database + "." + table + " FORMAT JSONEachRow";

    return ch.post()
        .uri(uriBuilder -> {
          uriBuilder.path("/");
          uriBuilder.queryParam("query", query);
          if (username != null && !username.isBlank()) uriBuilder.queryParam("user", username);
          if (password != null && !password.isBlank()) uriBuilder.queryParam("password", password);
          return uriBuilder.build();
        })
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(oneJsonLine)
        .retrieve()
        .onStatus(s -> !s.is2xxSuccessful(), resp ->
            resp.bodyToMono(String.class)
                .defaultIfEmpty("ClickHouse returned non-2xx")
                .flatMap(body -> Mono.error(new RuntimeException("ClickHouse error: " + body)))
        )
        .bodyToMono(String.class)
        .then();
  }
}
