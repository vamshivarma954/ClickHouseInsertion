package com.example.api;

import java.time.OffsetDateTime;

/** All fields map 1:1 to the ClickHouse table columns. Use null for optional ones. */
public class InsertOneFullRequest {
  public Integer message_length;
  public Integer message_sequence;
  public Integer stationary_kavach_id;
  public Integer nms_system_id;
  public Integer system_version;
  public String  message_date;                  // "DD/MM/YY"
  public String  message_time;                  // "HH:mm:ss"
  public String  crc;
  public String  specific_protocol;
  public String  packet_name;
  public Integer sender_identifier;
  public Integer receiver_identifier;
  public Integer packet_message_length;
  public Integer frame_number;
  public Integer packet_message_sequence;
  public Integer border_rfid_tag;
  public Integer ref_profile_id;
  public Integer onboard_kavach_identity;
  public String  sub_pkt_type;
  public Integer sub_pkt_len_ma;
  public String  frame_offset;
  public String  dst_loco_sos;
  public String  train_section_type;
  public String  line_number;
  public String  line_name;
  public String  type_of_signal;
  public Integer signal_ov;
  public String  stop_signal;
  public String  current_sig_aspect;
  public String  next_sig_aspect;
  public Integer approaching_signal_distance;
  public String  authority_type;
  public Integer authorized_speed;
  public Integer ma_wrt_sig;
  public String  req_shorten_ma;
  public Integer new_ma;
  public String  train_length_info_sts;
  public String  trn_len_info_type;
  public Integer ref_frame_num_tlm;
  public Integer ref_offset_int_tlm;
  public String  next_stn_comm;
  public Integer appr_stn_ilc_ibs_id;
  public String  sub_pkt_type_ssp;
  public Integer sub_pkt_len_ssp;
  public Integer ssp_count_info;
  public String  classified_speed_info;       // JSON as String
  public String  sub_pkt_type_gp;
  public Integer sub_pkt_len_gp;
  public Integer gp_count_info;
  public String  gradient_info;               // JSON as String
  public String  sub_pkt_type_lc;
  public Integer sub_pkt_len_lc;
  public Integer lm_count_info;
  public String  lc_info;                     // JSON or text
  public String  sub_pkt_type_tsp;
  public Integer sub_pkt_len_tsp;
  public Integer to_count_info;
  public String  speed_info;                  // JSON or text
  public String  sub_pkt_type_tag;
  public Integer sub_pkt_len_tag;
  public Integer dist_dup_tag;
  public Integer route_rfid_cnt;
  public String  rfid_info_list;              // JSON
  public String  abs_loc_reset;
  public Integer start_dist_to_loc_reset;
  public String  adj_loc_dir;
  public Integer abs_loc_correction;
  public Integer adj_line_cnt;
  public Integer self_tin;
  public String  self_tin_list;               // JSON
  public String  sub_pkt_type_tcp;
  public Integer sub_pkt_len_tcp;
  public Integer track_cond_cnt;
  public String  track_cond_info;             // JSON or text
  public String  sub_packet_tsrp;
  public Integer sub_packet_length_tsrp;
  public String  tsr_status;
  public Integer tsr_count;
  public String  tsr_info;                    // JSON
  public String  mac_code;

  /** These two map to ClickHouse DateTime columns. Supply ISO-8601 (e.g. 2025-08-03T12:34:56Z). */
  public OffsetDateTime created_at;
  public OffsetDateTime message_datetime;
}
