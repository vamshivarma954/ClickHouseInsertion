package com.example.api;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;
import com.example.api.InsertOneFullRequest;

@RestController
@RequestMapping("/api")
public class InsertFullController {
  private final JdbcTemplate jdbc;
  public InsertFullController(JdbcTemplate jdbc) { this.jdbc = jdbc; }

  @PostMapping(value="/insert-one-full", consumes="application/json")
  public ResponseEntity<?> insertOneFull(@RequestBody InsertOneFullRequest r) {
    // Ensure lc_info is not null for NOT NULL column
    if (r.lc_info == null) {
      r.lc_info = "{}"; // static value, can be changed as needed
    }
    final String sql = """
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
              ?,?,?,?, ?,
              ?,?,?,?, ?,
              ?,?,?,?, ?,?,
              ?,?,?,?, ?,?,
              ?,?,?,?, ?,
              ?,?,?,?, ?,?,
              ?,?)
      """;

    int n = jdbc.update(sql,
      r.message_length, r.message_sequence, r.stationary_kavach_id, r.nms_system_id, r.system_version,
      r.message_date, r.message_time, r.crc, r.specific_protocol, r.packet_name,
      r.sender_identifier, r.receiver_identifier, r.packet_message_length, r.frame_number, r.packet_message_sequence,
      r.border_rfid_tag, r.ref_profile_id, r.onboard_kavach_identity, r.sub_pkt_type, r.sub_pkt_len_ma,
      r.frame_offset, r.dst_loco_sos, r.train_section_type, r.line_number, r.line_name,
      r.type_of_signal, r.signal_ov, r.stop_signal, r.current_sig_aspect, r.next_sig_aspect,
      r.approaching_signal_distance, r.authority_type, r.authorized_speed, r.ma_wrt_sig, r.req_shorten_ma,
      r.new_ma, r.train_length_info_sts, r.trn_len_info_type, r.ref_frame_num_tlm, r.ref_offset_int_tlm,
      r.next_stn_comm, r.appr_stn_ilc_ibs_id, r.sub_pkt_type_ssp, r.sub_pkt_len_ssp, r.ssp_count_info,
      r.classified_speed_info, r.sub_pkt_type_gp, r.sub_pkt_len_gp, r.gp_count_info, r.gradient_info,
      r.sub_pkt_type_lc, r.sub_pkt_len_lc, r.lm_count_info, r.lc_info, r.sub_pkt_type_tsp,
      r.sub_pkt_len_tsp, r.to_count_info, r.speed_info, r.sub_pkt_type_tag, r.sub_pkt_len_tag,
      r.dist_dup_tag, r.route_rfid_cnt, r.rfid_info_list, r.abs_loc_reset, r.start_dist_to_loc_reset,
      r.adj_loc_dir, r.abs_loc_correction, r.adj_line_cnt, r.self_tin, r.self_tin_list,
      r.sub_pkt_type_tcp, r.sub_pkt_len_tcp, r.track_cond_cnt, r.track_cond_info, r.sub_packet_tsrp,
      r.sub_packet_length_tsrp, r.tsr_status, r.tsr_count, r.tsr_info, r.mac_code,
      r.created_at, r.message_datetime
    );

    return (n == 1) ? ResponseEntity.status(201).body("ok")
                    : ResponseEntity.internalServerError().body("failed");
  }
}
