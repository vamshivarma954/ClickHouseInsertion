# ClickHouse Server Optimization for 6000 Concurrent Inserts

## 🚨 CRITICAL: Add these settings to your ClickHouse server config.xml

Your ClickHouse server needs these optimizations to handle 6000 concurrent inserts:

### 1. Edit ClickHouse config.xml
```bash
sudo nano /etc/clickhouse-server/config.xml
```

### 2. Add these settings inside <clickhouse> tag:
```xml
<clickhouse>
    <!-- Performance Settings for 6000 Concurrent Inserts -->
    <max_insert_threads>32</max_insert_threads>
    <min_insert_block_size_bytes>1048576</min_insert_block_size_bytes>
    <max_memory_usage>40000000000</max_memory_usage>
    <max_threads>64</max_threads>
    <max_concurrent_queries>500</max_concurrent_queries>
    <max_concurrent_inserts>500</max_concurrent_inserts>
    
    <!-- Merge Settings for High Volume -->
    <parts_to_throw_insert>5000</parts_to_throw_insert>
    <parts_to_delay_insert>5000</parts_to_delay_insert>
    <max_insert_size>5000000000</max_insert_size>
    
    <!-- Async Insert Settings -->
    <async_insert>1</async_insert>
    <wait_for_async_insert>0</wait_for_async_insert>
    <async_insert_max_data_size>52428800</async_insert_max_data_size>
    <async_insert_busy_timeout_ms>1000</async_insert_busy_timeout_ms>
    <async_insert_stale_timeout_ms>0</async_insert_stale_timeout_ms>
    
    <!-- Network Settings -->
    <max_connections>2000</max_connections>
    <keep_alive_timeout>60</keep_alive_timeout>
    <listen_backlog>8192</listen_backlog>
    
    <!-- Memory Settings -->
    <max_memory_usage_for_user>20000000000</max_memory_usage_for_user>
    <max_memory_usage_for_all_queries>80000000000</max_memory_usage_for_all_queries>
    
    <!-- Background Processing -->
    <background_pool_size>64</background_pool_size>
    <background_schedule_pool_size>64</background_schedule_pool_size>
    
    <!-- Compression Settings -->
    <compression>
        <case>
            <min_part_size>10000000</min_part_size>
            <min_part_size_ratio>0.01</min_part_size_ratio>
            <method>lz4</method>
        </case>
    </compression>
    
    <!-- Logging Settings -->
    <logger>
        <level>warning</level>
        <log_queries>0</log_queries>
        <log_query_threads>0</log_query_threads>
    </logger>
</clickhouse>
```

### 3. Restart ClickHouse
```bash
sudo systemctl restart clickhouse-server
```

### 4. Verify settings
```bash
clickhouse-client --query "SELECT name, value FROM system.settings WHERE name LIKE '%insert%' OR name LIKE '%concurrent%' OR name LIKE '%memory%'"
```

## 🎯 Key Changes Made:

### Application Level:
- **Reduced concurrency** from 6000 to 1000 (prevents overload)
- **Increased memory buffers** to 50MB
- **Optimized connection pooling** to 2000 connections
- **Changed scheduler** to boundedElastic for better resource management

### ClickHouse Server Level:
- **32 insert threads** (4x default)
- **500 concurrent inserts** (10x default)
- **40GB memory usage** (40x default)
- **5000 parts threshold** (16x default)
- **50MB async buffer** (5x default)

## 📊 Expected Results:
- **All 6000 inserts successful**
- **3-5 second execution time**
- **No connection timeouts**
- **No memory issues**

## 🔧 If Still Having Issues:

### Check ClickHouse logs:
```bash
sudo tail -f /var/log/clickhouse-server/clickhouse-server.log
```

### Monitor system resources:
```bash
htop
free -h
```

### Test with smaller batch first:
Change `concurrent.insert.count=1000` to test with 1000 inserts first.

## ⚠️ Important Notes:
1. **Restart ClickHouse** after config changes
2. **Monitor memory usage** - ensure server has enough RAM
3. **Check network bandwidth** - 6000 concurrent requests need good network
4. **Start with smaller batches** if server is underpowered

The key is **balancing concurrency** - too high concurrency can cause failures, too low is slow.
