@echo off
echo ========================================
echo Testing 6000 Individual Concurrent Inserts
echo ========================================
echo.
echo This will test 6000 individual HTTP requests to ClickHouse
echo Each request contains 1 record (simulating different table scenario)
echo Optimized for FAST COMPLETION with your ClickHouse configuration
echo.
echo Starting test...
echo.

curl -X POST http://localhost:8080/api/insert-6000-native-concurrent

echo.
echo ========================================
echo Test completed!
echo ========================================
pause
