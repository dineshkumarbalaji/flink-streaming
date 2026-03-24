@echo off
echo Starting Antheon Flink Streaming Application...

REM Add Docker to PATH if not already present
SET DOCKER_PATH=C:\Program Files\Docker\Docker\resources\bin
IF NOT EXIST "%DOCKER_PATH%\docker.exe" (
    echo [ERROR] Docker not found at %DOCKER_PATH%. Please install Docker Desktop.
    pause
    exit /b 1
)
SET PATH=%DOCKER_PATH%;%PATH%

echo Verifying Docker is running...
docker ps >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Docker daemon is not running. Please start Docker Desktop and try again.
    pause
    exit /b 1
)

echo Building and deploying services with Docker Compose...
docker compose build --no-cache flink-app
if %ERRORLEVEL% NEQ 0 goto error
docker compose up -d

if %ERRORLEVEL% NEQ 0 goto error

echo.
echo ========================================================
echo   Application Started Successfully!
echo ========================================================
echo   Flink Dashboard:      http://localhost:8081
echo   Kafka UI:             http://localhost:8090
echo   Flink Control App:    http://localhost:8082
echo   Flink Control API:    http://localhost:8082/api/jobs/list
echo   ZooKeeper:            localhost:2181
echo ========================================================
echo.
pause
goto end

:error
echo.
echo [ERROR] Failed to start application. Please check the build logs above.
pause
exit /b 1

:end
