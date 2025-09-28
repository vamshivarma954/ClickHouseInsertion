package com.example.config;

import io.netty.channel.ChannelOption;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
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
    
    @Bean
    public WebClient optimizedClickHouseClient() {
        // Connection pool for maximum performance
     // Large connection pool: 2000 sockets, pending queue 10000
     ConnectionProvider connectionProvider = ConnectionProvider.builder("clickhouse-pool")
         .maxConnections(2000)  // 2000 sockets
         .pendingAcquireMaxCount(10000) // pending queue for bursts
         .maxIdleTime(Duration.ofSeconds(30))
         .maxLifeTime(Duration.ofSeconds(60))
         .pendingAcquireTimeout(Duration.ofSeconds(30))
         .evictInBackground(Duration.ofSeconds(120))
         .build();
        
        // Optimized HTTP client
    HttpClient httpClient = HttpClient.create(connectionProvider)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 30000)  // 30 second connection timeout
        .option(ChannelOption.SO_KEEPALIVE, true)
        .option(ChannelOption.TCP_NODELAY, true)  // Disable Nagle's algorithm
        .responseTimeout(Duration.ofSeconds(60))  // 60 second response timeout
        .doOnConnected(conn -> 
            conn.addHandlerLast(new ReadTimeoutHandler(60, TimeUnit.SECONDS))
            .addHandlerLast(new WriteTimeoutHandler(60, TimeUnit.SECONDS))
        );
        
        return WebClient.builder()
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .codecs(configurer -> {
                    configurer.defaultCodecs().maxInMemorySize(1024 * 1024); // 1MB buffer
                    configurer.defaultCodecs().enableLoggingRequestDetails(false);
                })
                .build();
    }
}
