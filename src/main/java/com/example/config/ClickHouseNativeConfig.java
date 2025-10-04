package com.example.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.net.URI;
import java.util.Objects;

@Configuration
public class ClickHouseNativeConfig {

	@Value("${clickhouse.host:}")
	private String host;

	@Value("${clickhouse.port:0}")
	private int port;

	@Value("${clickhouse.database}")
	private String database;

	@Value("${clickhouse.username}")
	private String username;

	@Value("${clickhouse.password}")
	private String password;


	@Value("${clickhouse.url:}")
	private String httpUrl; // optional, e.g., http://host:8123

	@Bean(destroyMethod = "close")
	public Connection clickHouseConnection() throws Exception {
		String resolvedHost = host;
		int resolvedPort = port;
		if (httpUrl != null && !httpUrl.isBlank()) {
			URI uri = URI.create(httpUrl);
			resolvedHost = (resolvedHost == null || resolvedHost.isBlank()) ? uri.getHost() : resolvedHost;
			// Always prefer HTTP port from URL for JDBC HTTP connection
			int httpPort = uri.getPort() > 0 ? uri.getPort() : 8123;
			resolvedPort = httpPort;
		}
		if (resolvedHost == null || resolvedHost.isBlank()) {
			throw new IllegalStateException("clickhouse.host or clickhouse.url must be configured");
		}
		String url = String.format("jdbc:clickhouse://%s:%d/%s", resolvedHost, resolvedPort, database);
		return DriverManager.getConnection(url, username, password);
	}
}


