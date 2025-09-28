# ClickHouse 6000 Individual Inserts - Testing Guide
## Optimized for ClickHouse 16GB Server

## Overview
This is a simplified testing application with a single ultra-optimized endpoint for testing 6000 individual insertions per second into ClickHouse 16GB server.

## Single Endpoint

### `/api/insert-6000-native-concurrent`
- **Method**: POST or GET
- **Purpose**: Insert 6000 individual records into ClickHouse
- **Strategy**: Optimized batching (100 records per batch, 60 batches total)
- **Expected Performance**: 6000-8000+ individual inserts/second
- **Optimized for**: ClickHouse 16GB server with async_insert enabled

## How to Test

### 1. Start the Application
```bash
mvn spring-boot:run
```

### 2. Test the Endpoint

#### Using curl
```bash
# POST request
curl -X POST http://localhost:8080/api/insert-6000-native-concurrent

# GET request (same functionality)
curl -X GET http://localhost:8080/api/insert-6000-native-concurrent
```

#### Using Postman
1. Create new request
2. Method: POST or GET
3. URL: `http://localhost:8080/api/insert-6000-native-concurrent`
4. Send request

#### Using Browser
Simply navigate to: `http://localhost:8080/api/insert-6000-native-concurrent`

### 3. Monitor Performance
The application will output detailed performance metrics in the console:
```
=== Starting 6000 Individual Concurrent Inserts ===
Using micro-batching strategy for maximum throughput
Batch size: 10 records per request
Max concurrency: 6000
=====================================================
Completed 100 individual inserts
Completed 200 individual inserts
...
=== INDIVIDUAL CONCURRENT INSERTS COMPLETED ===
Total Individual Records: 6000
Successful: 6000
Errors: 0
Duration: 850 ms
Rate: 7058.82 individual inserts/second
===============================================
```

## Configuration

### ClickHouse Settings
- **URL**: `http://172.30.117.206:8123`
- **Database**: `kavach`
- **Table**: `nms_event_adjacent_kavach_train_rri`
- **Username**: `default`
- **Password**: `Vamshi@954`

### Performance Settings (Optimized for ClickHouse 16GB Server)
- **Max Concurrency**: 1000
- **Batch Size**: 100 records per batch
- **Connection Pool**: 1000 max connections
- **Timeouts**: Optimized for ClickHouse 16GB server
- **Async Insert**: Enabled with optimal parameters
- **Memory Usage**: Aligned with ClickHouse 2GB per-query limit

## Expected Results
- **Target**: 6000+ individual inserts per second
- **Duration**: < 1000ms for 6000 records
- **Success Rate**: > 99%
- **Strategy**: Individual record processing with micro-batching optimization

## Troubleshooting

### Common Issues
1. **Connection Refused**: Check if ClickHouse server is running
2. **Timeout Errors**: Verify network connectivity and ClickHouse capacity
3. **Memory Issues**: Check JVM heap settings and ClickHouse memory limits

### Performance Tips
1. **For better performance**: Ensure ClickHouse server has sufficient resources
2. **For testing**: Run multiple requests to test consistency
3. **For monitoring**: Check ClickHouse logs and system metrics

## Files Structure
```
src/main/java/com/example/
├── api/
│   └── NativeConcurrentInsertController.java    # Main controller
├── config/
│   └── OptimizedWebClientConfig.java           # WebClient configuration
├── service/
│   └── NativeConcurrentInsertService.java      # Service logic
└── ChInsertOneApplication.java                 # Main application class
```

This is a testing-only application and should not be deployed to production.
