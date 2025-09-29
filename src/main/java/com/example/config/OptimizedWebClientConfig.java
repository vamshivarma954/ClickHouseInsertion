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
        // Optimized connection pool for 6000 individual inserts/sec - BALANCED PERFORMANCE
        ConnectionProvider connectionProvider = ConnectionProvider.builder("clickhouse-individual-pool")
            .maxConnections(600)  // Balanced for ClickHouse server capacity
            .pendingAcquireMaxCount(1200) // Slightly larger pending queue
            .maxIdleTime(Duration.ofSeconds(8)) // Balanced idle timeout
            .maxLifeTime(Duration.ofSeconds(25)) // 25 second connection lifetime
            .pendingAcquireTimeout(Duration.ofSeconds(2)) // Faster timeout
            .evictInBackground(Duration.ofSeconds(8)) // More frequent eviction
            .build();
        
        // Optimized HTTP client for 6000 individual inserts/sec - REALISTIC SETTINGS
        HttpClient httpClient = HttpClient.create(connectionProvider)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)  // 5 second connection timeout
            .option(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.TCP_NODELAY, true)  // Disable Nagle's algorithm
            .option(ChannelOption.SO_REUSEADDR, true) // Reuse addresses
            .option(ChannelOption.SO_RCVBUF, 4096) // 4KB receive buffer
            .option(ChannelOption.SO_SNDBUF, 4096) // 4KB send buffer
            .responseTimeout(Duration.ofSeconds(120))  // 120 second response timeout for long SELECTs
            .doOnConnected(conn -> 
                conn.addHandlerLast(new ReadTimeoutHandler(120, TimeUnit.SECONDS))
                .addHandlerLast(new WriteTimeoutHandler(120, TimeUnit.SECONDS))
            );
        
        return WebClient.builder()
                .baseUrl(clickhouseUrl)
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .codecs(configurer -> {
                    configurer.defaultCodecs().maxInMemorySize(512 * 1024 * 1024); // 512MB buffer for large SELECT responses
                    configurer.defaultCodecs().enableLoggingRequestDetails(false);
                })
                .build();
    }
}
