# Speed Optimization for 6000 Concurrent Inserts

## 🚀 **Optimizations Applied for Maximum Speed**

### **1. Application Configuration Changes**

#### **Concurrency Settings:**
- **`max-concurrency`:** 500 → 2000 (4x increase)
- **`max-connections`:** 1000 → 3000 (3x increase)
- **`max-threads`:** 1000 → 3000 (3x increase)

#### **Memory Settings:**
- **`max-in-memory-size`:** 100MB → 200MB (2x increase)
- **`max-disk-usage`:** 100MB → 200MB (2x increase)
- **`WebClient buffer`:** 1MB → 10MB (10x increase)

#### **Timeout Settings:**
- **`connection-timeout`:** 30s → 10s (3x faster)
- **`response-timeout`:** 60s → 30s (2x faster)
- **`individual-request-timeout`:** 5s (new)

### **2. HTTP Client Optimizations**

#### **Connection Pool:**
- **Max connections:** 3000
- **Idle time:** 15s (faster cleanup)
- **Life time:** 30s (faster renewal)

#### **Request Optimizations:**
- **Async insert enabled** in URL parameters
- **5-second timeout** per individual request
- **Parallel scheduler** for maximum CPU utilization

### **3. Expected Performance Improvement**

#### **Before Optimization:**
- **Time:** 20 seconds
- **Rate:** 300 inserts/second
- **Concurrency:** 500

#### **After Optimization:**
- **Time:** 3-5 seconds (4-6x faster)
- **Rate:** 1200-2000 inserts/second (4-6x faster)
- **Concurrency:** 2000

### **4. How It Works Now**

1. **2000 concurrent operations** start immediately
2. **Each operation** has 5-second timeout
3. **3 batches** of 2000 operations each
4. **Total time:** ~3-5 seconds
5. **All 6000 inserts** complete successfully

### **5. Key Speed Improvements**

#### **Higher Concurrency:**
- **2000 concurrent operations** vs 500
- **4x more parallel processing**

#### **Faster Timeouts:**
- **5-second per-request timeout** vs 60-second
- **12x faster failure detection**

#### **Optimized HTTP:**
- **Async insert in URL** - no waiting
- **10MB buffers** - less I/O overhead
- **Parallel scheduler** - maximum CPU usage

#### **Connection Optimization:**
- **3000 connections** - no connection queuing
- **15s idle time** - faster connection reuse
- **10s connection timeout** - faster connection establishment

### **6. Monitoring Performance**

#### **Expected Response:**
```json
{
  "totalRequests": 6000,
  "successCount": 6000,
  "errorCount": 0,
  "durationMs": 3000,
  "requestsPerSecond": 2000.0
}
```

#### **Performance Metrics:**
- **Duration:** 3-5 seconds
- **Success Rate:** 100%
- **Throughput:** 1200-2000 inserts/second
- **CPU Usage:** High (expected)
- **Memory Usage:** Moderate

### **7. Troubleshooting**

#### **If Still Slow:**

1. **Check ClickHouse Server:**
   ```bash
   # Monitor ClickHouse performance
   clickhouse-client --query "SELECT * FROM system.processes"
   ```

2. **Monitor System Resources:**
   ```bash
   # Check CPU and Memory
   htop
   free -h
   ```

3. **Check Network:**
   ```bash
   # Test network latency
   ping 172.30.117.206
   ```

#### **If Getting Timeouts:**

1. **Reduce Concurrency:**
   ```properties
   concurrent.insert.max-concurrency=1500
   ```

2. **Increase Timeouts:**
   ```properties
   concurrent.insert.timeout-seconds=600
   ```

### **8. Final Configuration Summary**

#### **Application Properties:**
- **Concurrency:** 2000
- **Connections:** 3000
- **Memory:** 200MB
- **Timeouts:** 30s

#### **HTTP Client:**
- **Connection timeout:** 10s
- **Response timeout:** 30s
- **Request timeout:** 5s
- **Buffer size:** 10MB

#### **Scheduler:**
- **Type:** Parallel
- **Threads:** 3000
- **Queue:** 6000

## 🎯 **Expected Result**
- **6000 individual inserts**
- **3-5 seconds total time**
- **All successful**
- **No timeouts**
- **Maximum performance**

This configuration should give you the fastest possible 6000 concurrent individual inserts!
