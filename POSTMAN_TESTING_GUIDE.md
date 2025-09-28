# Postman Testing Guide - ClickHouse Insert APIs

## 🎯 Available API Endpoints

You have **TWO API endpoints** available:

1. **`/api/insert-one`** - Single record insertion
2. **`/api/insert-6000-concurrent`** - 6000 concurrent individual insertions

## 🚀 API Endpoints

### **1. Single Record Insert**
- **Method**: POST
- **URL**: `http://localhost:8080/api/insert-one`
- **Content-Type**: `application/json`
- **Body**: 
```json
{
  "messageDatetime": "2025-01-15 10:15:30",
  "subPktType": "0000",
  "stationaryKavachId": 50001,
  "messageSequence": 1000
}
```

### **2. 6000 Concurrent Inserts**
- **Method**: POST (or GET for testing)
- **URL**: `http://localhost:8080/api/insert-6000-concurrent`
- **Content-Type**: `application/json`
- **Body**: No body required (empty JSON `{}`)

## 📋 Postman Setup

### Step 1: Create New Request
1. Open Postman
2. Create new request
3. Set method to **POST**
4. Set URL to: `http://localhost:8080/api/insert-6000-concurrent`

### Step 2: Headers (Optional)
```
Content-Type: application/json
```

### Step 3: Body (Optional)
```json
{}
```

### Step 4: Send Request
Click **Send** button

## 🔄 What Happens When You Hit the API

1. **Single API Call**: You make ONE request from Postman
2. **6000 Individual Inserts**: The API internally performs 6000 separate database insertions
3. **Concurrent Execution**: All 6000 inserts run concurrently (not sequentially)
4. **Real-time Progress**: Console shows progress every 100 inserts
5. **Performance Metrics**: Final response includes timing and success/error counts

## 📊 Expected Response

```json
{
  "totalRequests": 6000,
  "successCount": 5987,
  "errorCount": 13,
  "durationMs": 15432,
  "requestsPerSecond": 388.12
}
```

## 🖥️ Console Output

When you hit the API, you'll see:

```
=== 6000 Concurrent Individual Inserts Started ===
Single API call will perform 6000 individual database insertions
Each insert is a separate database operation
==================================================

Starting 6000 concurrent individual database inserts...
Completed 100 inserts
Completed 200 inserts
Completed 300 inserts
...
Completed 5900 inserts
Completed 6000 inserts

=== CONCURRENT INSERT COMPLETED ===
Concurrent Insert Results:
  Total Requests: 6000
  Successful: 5987
  Errors: 13
  Duration: 15432 ms
  Rate: 388.12 inserts/second
====================================
```

## ⚙️ Configuration

The system is configured for:
- **6000 individual inserts** (not batch)
- **200 concurrent operations** at a time
- **300 second timeout** per operation
- **Direct database operations** (no HTTP calls)

## 🧪 Testing Steps

1. **Start the Application**:
   ```bash
   mvn spring-boot:run
   ```

2. **Open Postman**:
   - Create new request
   - Set method to POST
   - Set URL to: `http://localhost:8080/api/insert-6000-concurrent`
   - Click Send

3. **Monitor Progress**:
   - Watch console for progress updates
   - Wait for completion (usually 10-30 seconds)
   - Check response for final metrics

## 🔍 Verification

After the API completes:

1. **Check Database**: Query ClickHouse to verify 6000 records were inserted
2. **Check Response**: Verify success count in Postman response
3. **Check Console**: Review performance metrics in application logs

## 🚨 Troubleshooting

### If API Times Out:
- Increase timeout in `application.properties`
- Reduce `concurrent.insert.max-concurrency`

### If Database Connection Fails:
- Verify ClickHouse is running
- Check connection settings in `application.properties`

### If Memory Issues:
- Reduce `concurrent.insert.max-concurrency`
- Increase JVM heap size

## 📈 Performance Expectations

- **Duration**: 10-30 seconds for 6000 inserts
- **Success Rate**: 95-99% (some failures are normal)
- **Throughput**: 200-500 inserts/second
- **Memory Usage**: Moderate (depends on concurrency setting)

## 🎯 Key Benefits

✅ **Single API Call**: One request from Postman  
✅ **6000 Individual Inserts**: Each is a separate database operation  
✅ **Concurrent Execution**: All inserts run in parallel  
✅ **Real-time Monitoring**: Progress updates in console  
✅ **Performance Metrics**: Detailed timing and success rates  
✅ **Error Handling**: Continues even if some inserts fail  

## 🔧 Customization

You can modify the behavior by changing:

- `concurrent.insert.count=6000` - Number of inserts
- `concurrent.insert.max-concurrency=200` - Concurrent operations
- `concurrent.insert.timeout-seconds=300` - Timeout per operation

## 📝 Example Postman Collection

```json
{
  "info": {
    "name": "6000 Concurrent Inserts",
    "description": "Single API call for 6000 concurrent individual inserts"
  },
  "item": [
    {
      "name": "Insert 6000 Concurrent",
      "request": {
        "method": "POST",
        "header": [
          {
            "key": "Content-Type",
            "value": "application/json"
          }
        ],
        "url": {
          "raw": "http://localhost:8080/api/insert-6000-concurrent",
          "protocol": "http",
          "host": ["localhost"],
          "port": "8080",
          "path": ["api", "insert-6000-concurrent"]
        },
        "body": {
          "mode": "raw",
          "raw": "{}"
        }
      }
    }
  ]
}
```

This setup gives you exactly what you need: **one API call from Postman that performs 6000 individual concurrent insertions** to ClickHouse!
