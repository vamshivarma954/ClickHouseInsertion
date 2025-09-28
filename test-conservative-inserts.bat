@echo off
echo ========================================
echo Testing Conservative Individual Inserts
echo ========================================
echo.
echo This will test with more conservative settings:
echo - 200 concurrent connections (instead of 1500)
echo - 10 second timeouts (instead of 3 seconds)
echo - 2 retries on failure
echo - 500 max connections
echo.
echo Starting conservative test...
echo.

curl -X POST http://localhost:8080/api/insert-6000-native-concurrent

echo.
echo ========================================
echo Conservative test completed!
echo ========================================
pause
