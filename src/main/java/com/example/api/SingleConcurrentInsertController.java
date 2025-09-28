package com.example.api;

import com.example.service.ConcurrentDatabaseInsertService;
import com.example.service.ConcurrentDatabaseInsertService.ConcurrentInsertResult;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

/**
 * Single API endpoint for 6000 concurrent individual inserts
 * Hit this endpoint from Postman to trigger 6000 concurrent database insertions
 */
@RestController
@RequestMapping("/api")
public class SingleConcurrentInsertController {
    
    private final ConcurrentDatabaseInsertService concurrentService;
    
    public SingleConcurrentInsertController(ConcurrentDatabaseInsertService concurrentService) {
        this.concurrentService = concurrentService;
    }
    
    /**
     * Single API endpoint that performs 6000 concurrent individual inserts
     * 
     * POST /api/insert-6000-concurrent
     * 
     * This endpoint:
     * - Makes a single API call from Postman
     * - Internally performs 6000 individual concurrent database insertions
     * - Each insert is a separate database operation (not batch)
     * - Returns performance metrics
     */
    @PostMapping("/insert-6000-concurrent")
    public Mono<ConcurrentInsertResult> insert6000Concurrent() {
        System.out.println("=== 6000 Concurrent Individual Inserts Started ===");
        System.out.println("Single API call will perform 6000 individual database insertions");
        System.out.println("Each insert is a separate database operation");
        System.out.println("==================================================");
        
        return concurrentService.execute6000ConcurrentInserts()
                .doOnSuccess(result -> {
                    System.out.println("\n=== CONCURRENT INSERT COMPLETED ===");
                    System.out.println(result);
                    System.out.println("====================================");
                })
                .doOnError(error -> {
                    System.err.println("Error in concurrent inserts: " + error.getMessage());
                });
    }
    
    /**
     * GET endpoint for testing
     * GET /api/insert-6000-concurrent
     */
    @GetMapping("/insert-6000-concurrent")
    public Mono<ConcurrentInsertResult> insert6000ConcurrentGet() {
        return insert6000Concurrent();
    }
}
