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
        // Ultra-optimized connection pool for ClickHouse 16GB server
        ConnectionProvider connectionProvider = ConnectionProvider.builder("clickhouse-16gb-pool")
            .maxConnections(800)  // Optimized for ClickHouse 16GB server (aligned with max_threads=4)
            .pendingAcquireMaxCount(1600) // Reasonable pending queue
            .maxIdleTime(Duration.ofSeconds(15)) // Aligned with server settings
            .maxLifeTime(Duration.ofSeconds(45)) // 45 second connection lifetime
            .pendingAcquireTimeout(Duration.ofSeconds(2)) // Fast timeout
            .evictInBackground(Duration.ofSeconds(15)) // Regular eviction
            .build();
        
        // Ultra-optimized HTTP client for ClickHouse 16GB server
        HttpClient httpClient = HttpClient.create(connectionProvider)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 1500)  // 1.5 second connection timeout
            .option(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.TCP_NODELAY, true)  // Disable Nagle's algorithm
            .option(ChannelOption.SO_REUSEADDR, true) // Reuse addresses
            .option(ChannelOption.SO_RCVBUF, 8192) // 8KB receive buffer (optimized for 16GB server)
            .option(ChannelOption.SO_SNDBUF, 8192) // 8KB send buffer
            .responseTimeout(Duration.ofSeconds(8))  // 8 second response timeout
            .doOnConnected(conn -> 
                conn.addHandlerLast(new ReadTimeoutHandler(8, TimeUnit.SECONDS))
                .addHandlerLast(new WriteTimeoutHandler(8, TimeUnit.SECONDS))
            );
        
        return WebClient.builder()
                .baseUrl(clickhouseUrl)
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .codecs(configurer -> {
                    configurer.defaultCodecs().maxInMemorySize(80 * 1024 * 1024); // 80MB buffer (optimized for 16GB server)
                    configurer.defaultCodecs().enableLoggingRequestDetails(false);
                })
                .build();
    }
}
