@echo off
echo ========================================
echo ClickHouse 6000 Inserts Performance Test
echo Optimized for ClickHouse 16GB Server
echo ========================================
echo.

echo Testing /api/insert-6000-native-concurrent endpoint...
echo Expected: 6000+ individual inserts per second
echo Batch size: 100 records per batch
echo Max concurrency: 1000
echo.

curl -X POST http://localhost:8080/api/insert-6000-native-concurrent
echo.
echo.

echo ========================================
echo Performance Test Complete
echo Check console logs for detailed metrics
echo ========================================
pause
