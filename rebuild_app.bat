@echo off
echo Rebuilding and Restarting Flink App ONLY...
docker-compose up -d --build flink-app
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Build failed!
    pause
    exit /b %ERRORLEVEL%
)
echo.
echo Flink App Rebuilt and Restarted!
echo.
pause
