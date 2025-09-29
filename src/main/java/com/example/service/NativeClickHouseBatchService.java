package com.example.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriComponentsBuilder;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;

@Service
public class NativeClickHouseBatchService {

    private final WebClient webClient;

    public NativeClickHouseBatchService(WebClient optimizedClickHouseClient) {
        this.webClient = optimizedClickHouseClient;
    }

    @Value("${clickhouse.username:default}")
    private String clickhouseUser;

    @Value("${clickhouse.password:}")
    private String clickhousePassword;

    @Value("${clickhouse.database:default}")
    private String clickhouseDb;

    @Value("${clickhouse.table:your_table}")
    private String clickhouseTable;

    /**
     * Insert 6000 rows in a single HTTP batch using ClickHouse JSONEachRow.
     */
    public void insert6000Batch() throws Exception {
        StringBuilder payload = new StringBuilder();
        com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        for (int i = 1; i <= 6000; i++) {
            Map<String, Object> row = createRow(i);
            payload.append(mapper.writeValueAsString(row)).append("\n");
        }

        String targetPath = UriComponentsBuilder.fromPath("/")
            .queryParam("query", "INSERT INTO " + clickhouseDb + "." + clickhouseTable + " FORMAT JSONEachRow")
            .build(false)
            .toUriString();

        webClient.post()
            .uri(targetPath)
            .headers(h -> h.setBasicAuth(clickhouseUser, clickhousePassword))
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(payload.toString())
            .retrieve()
            .toBodilessEntity()
            .block();
    }

    private Map<String, Object> createRow(int sequence) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("message_sequence", sequence);
        row.put("created_at", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        return row;
    }
}
