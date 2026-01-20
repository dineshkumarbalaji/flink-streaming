@echo off
echo ========================================================
echo        Setting up Kafka Topics and Data
echo ========================================================

echo.
echo [1/3] Creating topics...

:: Create Source Topic
docker exec kafka kafka-topics --create --topic source-topic1 --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --if-not-exists
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Failed to create source-topic1. Is Kafka running?
    goto :error
)
echo Created 'source-topic1'

:: Create Target Topic
docker exec kafka kafka-topics --create --topic target-topic1 --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --if-not-exists
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Failed to create target-topic1.
    goto :error
)
echo Created 'target-topic1'

echo.
echo [2/3] Sending sample data to 'source-topic1'...

:: Send generic JSON data
echo {"id": 1, "message": "Hello Flink 1", "timestamp": 1700000001} | docker exec -i kafka kafka-console-producer --topic source-topic1 --bootstrap-server localhost:9092
echo {"id": 2, "message": "Hello Flink 2", "timestamp": 1700000002} | docker exec -i kafka kafka-console-producer --topic source-topic1 --bootstrap-server localhost:9092
echo {"id": 3, "message": "Hello Flink 3", "timestamp": 1700000003} | docker exec -i kafka kafka-console-producer --topic source-topic1 --bootstrap-server localhost:9092

echo.
echo [3/3] Listing current topics in Kafka:
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

echo.
echo ========================================================
echo   Test setup complete!
echo   Data sent to: source-topic1
echo   Output expected in: target-topic1
echo ========================================================
pause
exit /b 0

:error
echo.
echo [FAIL] Verification failed. Please ensure 'start_app.bat' was run and Docker is healthy.
pause
exit /b 1
