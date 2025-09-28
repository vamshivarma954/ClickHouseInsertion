@echo off
echo ========================================
echo Testing ClickHouse Connection
echo ========================================
echo.
echo Testing basic connection to ClickHouse...
echo.

curl -X GET "http://172.30.117.206:8123/?query=SELECT%201"

echo.
echo ========================================
echo Connection test completed!
echo ========================================
pause
