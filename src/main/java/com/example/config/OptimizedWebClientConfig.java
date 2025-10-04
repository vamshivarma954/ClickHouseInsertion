package com.example.config;

import io.netty.channel.ChannelOption;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Configuration
public class OptimizedWebClientConfig {
    
    @Value("${clickhouse.url}")
    private String clickhouseUrl;
    
    @Bean
    public WebClient optimizedClickHouseClient() {
        // Balanced connection pool for streaming and inserts
        ConnectionProvider connectionProvider = ConnectionProvider.builder("clickhouse-pool")
            .maxConnections(8)  // Conservative for stability
            .pendingAcquireMaxCount(256) // Reasonable queue
            .maxIdleTime(Duration.ofMinutes(5)) // 5 minute idle timeout
            .maxLifeTime(Duration.ofMinutes(10)) // 10 minute connection lifetime
            .pendingAcquireTimeout(Duration.ofSeconds(30)) // Longer acquire timeout
            .evictInBackground(Duration.ofMinutes(2)) // Background eviction
            .build();
        
        // HTTP client optimized for long streaming responses with robust connection handling
        HttpClient httpClient = HttpClient.create(connectionProvider)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000)  // 10 second connection timeout
            .option(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.TCP_NODELAY, true)  // Disable Nagle's algorithm
            .option(ChannelOption.SO_REUSEADDR, true) // Reuse addresses
            .option(ChannelOption.SO_RCVBUF, 131072) // 128KB receive buffer
            .option(ChannelOption.SO_SNDBUF, 131072) // 128KB send buffer
            .option(ChannelOption.SO_LINGER, 0) // Disable SO_LINGER to avoid connection hanging
            .responseTimeout(Duration.ofHours(2))  // 2 hour response timeout for long streams
            .doOnConnected(conn -> 
                conn.addHandlerLast(new ReadTimeoutHandler(7200, TimeUnit.SECONDS))
                .addHandlerLast(new WriteTimeoutHandler(60, TimeUnit.SECONDS))
            )
            .doOnDisconnected(conn -> {
                System.out.println("Connection disconnected: " + conn.channel().remoteAddress());
            });
        
        return WebClient.builder()
                .baseUrl(clickhouseUrl)
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .codecs(configurer -> {
                    configurer.defaultCodecs().maxInMemorySize(1024 * 1024); // 1MB buffer to prevent memory issues
                    configurer.defaultCodecs().enableLoggingRequestDetails(false);
                })
                .build();
    }
}
