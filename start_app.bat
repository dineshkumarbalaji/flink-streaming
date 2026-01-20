@echo off
echo Starting Antheon Flink Streaming Application...
echo Building and deploying services with Docker Compose...

docker-compose up -d --build

if %ERRORLEVEL% NEQ 0 (
    echo.
    echo [ERROR] Failed to start application. Please check Docker is running.
    pause
    exit /b %ERRORLEVEL%
)

echo.
echo ========================================================
echo   Application Started Successfully!
echo ========================================================
echo   Flink UI:        http://localhost:8081
echo   Kafka UI:        http://localhost:8090
echo   Spring Boot App: http://localhost:8080
echo ========================================================
echo.
pause
