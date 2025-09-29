package com.example.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

@Service
public class ClickHouseBulkInsertService {

	private final WebClient webClient;

	public ClickHouseBulkInsertService(WebClient optimizedClickHouseClient) {
		this.webClient = optimizedClickHouseClient;
	}

	@Value("${clickhouse.username}")
	private String username;

	@Value("${clickhouse.password}")
	private String password;

	@Value("${clickhouse.database}")
	private String database;

	@Value("${clickhouse.table}")
	private String table;

	public void insertJsonEachRow(List<Map<String, Object>> rows) throws Exception {
		if (rows == null || rows.isEmpty()) {
			return;
		}

		StringBuilder payload = new StringBuilder();
		for (Map<String, Object> row : rows) {
			// Each line must be a JSON object; rely on ClickHouse server to parse
			String json = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(row);
			payload.append(json).append("\n");
		}

		String targetPath = UriComponentsBuilder.fromPath("/")
			.queryParam("query", "INSERT INTO " + database + "." + table + " FORMAT JSONEachRow")
			.queryParam("async_insert", "0")
			.build(false)
			.toUriString();

		Mono<Void> call = webClient.post()
			.uri(targetPath)
			.headers(h -> h.setBasicAuth(username, password))
			.contentType(MediaType.APPLICATION_JSON)
			.bodyValue(payload.toString())
			.retrieve()
			.toBodilessEntity()
			.then();

		call.block();
	}
}


