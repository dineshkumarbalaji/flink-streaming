@echo off
echo ========================================================
echo        Setting up Kafka Topics and Data
echo ========================================================

:: Configuration
set NAMESPACE=dvx
set LABEL_SELECTOR=app.kubernetes.io/name=flink-streaming
set KAFKA_BOOTSTRAP=localhost:9092

echo.
echo [0/3] Resolving flink-streaming pod name...
for /f "delims=" %%i in ('kubectl get pod -n %NAMESPACE% -l %LABEL_SELECTOR% -o jsonpath^="{.items[0].metadata.name}"') do set FLINK_POD=%%i

if "%FLINK_POD%"=="" (
    echo [ERROR] Could not find a running flink-streaming pod in namespace '%NAMESPACE%'.
    echo         Ensure the deployment is up: kubectl get pods -n %NAMESPACE% -l %LABEL_SELECTOR%
    goto :error
)
echo Found pod: %FLINK_POD%

echo.
echo [1/3] Creating topics...

:: Create Source Topic
kubectl exec %FLINK_POD% -n %NAMESPACE% -- kafka-topics --create --topic source-topic1 --bootstrap-server %KAFKA_BOOTSTRAP% --replication-factor 1 --partitions 1 --if-not-exists
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Failed to create source-topic1. Is Kafka reachable from the pod?
    goto :error
)
echo Created 'source-topic1'

:: Create Target Topic
kubectl exec %FLINK_POD% -n %NAMESPACE% -- kafka-topics --create --topic target-topic1 --bootstrap-server %KAFKA_BOOTSTRAP% --replication-factor 1 --partitions 1 --if-not-exists
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Failed to create target-topic1.
    goto :error
)
echo Created 'target-topic1'

echo.
echo [2/3] Sending sample data to 'source-topic1'...

echo {"id": 1, "message": "Hello Flink 1", "timestamp": 1700000001} | kubectl exec -i %FLINK_POD% -n %NAMESPACE% -- kafka-console-producer --topic source-topic1 --bootstrap-server %KAFKA_BOOTSTRAP%
echo {"id": 2, "message": "Hello Flink 2", "timestamp": 1700000002} | kubectl exec -i %FLINK_POD% -n %NAMESPACE% -- kafka-console-producer --topic source-topic1 --bootstrap-server %KAFKA_BOOTSTRAP%
echo {"id": 3, "message": "Hello Flink 3", "timestamp": 1700000003} | kubectl exec -i %FLINK_POD% -n %NAMESPACE% -- kafka-console-producer --topic source-topic1 --bootstrap-server %KAFKA_BOOTSTRAP%

echo.
echo [3/3] Listing current topics in Kafka:
kubectl exec %FLINK_POD% -n %NAMESPACE% -- kafka-topics --list --bootstrap-server %KAFKA_BOOTSTRAP%

echo.
echo ========================================================
echo   Test setup complete!
echo   Pod used:    %FLINK_POD%
echo   Data sent to: source-topic1
echo   Output expected in: target-topic1
echo ========================================================
pause
exit /b 0

:error
echo.
echo [FAIL] Verification failed. Check that the pod is running and Kafka is reachable.
echo        kubectl get pods -n %NAMESPACE% -l %LABEL_SELECTOR%
pause
exit /b 1
