@echo off
REM Production Deployment Script for Real Estate Data Pipeline
REM Windows-only deployment for local development

echo 🚀 Starting Real Estate Data Pipeline Deployment...
echo.

REM Check if MinIO_run.bat exists
if not exist "MinIO_run.bat" (
    echo ❌ Error: MinIO_run.bat not found!
    echo Please ensure MinIO is installed and MinIO_run.bat exists.
    pause
    exit /b 1
)

REM 1. Start MinIO Storage
echo 📦 Starting MinIO storage...
start "MinIO Server" MinIO_run.bat

REM 2. Wait for MinIO to be ready
echo ⏳ Waiting for MinIO to start (15 seconds)...
timeout /t 15 /nobreak > nul

REM 3. Check if we're in the right directory and navigate to pipeline folder
if exist "src\pipelines\real-estate" (
    cd src\pipelines\real-estate
    echo 📂 Changed to pipeline directory: %CD%
) else (
    echo ⚠️ Warning: Pipeline directory not found at expected location
    echo Current directory: %CD%
)

REM 4. Start Dagster Development Server
echo ⚙️ Starting Dagster development server...
echo Dagster will be available at: http://localhost:3000
dagster dev

echo.
echo ✅ Deployment complete!
echo 🌐 Access points:
echo   - Dagster UI: http://localhost:3000
echo   - MinIO Console: http://localhost:9001 (not 9000)
echo   - MinIO API: http://localhost:9000
echo   - MinIO Credentials: minioadmin/minioadmin
echo.
echo Press any key to exit...
pause > nul