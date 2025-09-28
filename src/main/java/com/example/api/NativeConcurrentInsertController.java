package com.example.api;

import com.example.service.NativeConcurrentInsertService;
import com.example.service.NativeConcurrentInsertService.NativeConcurrentResult;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

/**
 * Controller for 6000 individual concurrent insertions using ClickHouse native HTTP API
 * Each insert is a separate HTTP request to ClickHouse
 */
@RestController
@RequestMapping("/api")
public class NativeConcurrentInsertController {
    
    private final NativeConcurrentInsertService nativeService;
    
    public NativeConcurrentInsertController(NativeConcurrentInsertService nativeService) {
        this.nativeService = nativeService;
    }
    
    /**
     * 6000 individual concurrent insertions using ClickHouse native HTTP API
     * 
     * POST /api/insert-6000-native-concurrent
     * 
     * This endpoint:
     * - Makes 6000 individual HTTP requests to ClickHouse
     * - Each request inserts exactly 1 record
     * - Uses ClickHouse native HTTP API with JSONEachRow format
     * - All 6000 requests run concurrently
     * - Returns performance metrics
     */
    @PostMapping("/insert-6000-native-concurrent")
    public Mono<NativeConcurrentResult> insert6000NativeConcurrent() {
        return nativeService.insert6000IndividualConcurrent();
    }
    
    /**
     * GET endpoint for testing
     * GET /api/insert-6000-native-concurrent
     */
    @GetMapping("/insert-6000-native-concurrent")
    public Mono<NativeConcurrentResult> insert6000NativeConcurrentGet() {
        return insert6000NativeConcurrent();
    }
}
